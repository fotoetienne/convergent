(ns convergent.crdt
  (:require
   [clojure.set :refer [union difference]]
   #?(:clj [clojure.core.match :refer [match]]
      :cljs [cljs.core.match :refer-macros [match]])
   #?(:clj [clojure.core.reducers :as r])))


;;; Protocol

(defprotocol CRDT
  "CRDT protocol.

  This protocol defines a Convergent Replicated Data Type

  join - Necessary to define a join-semilattice. Joins two crdts of the same
         type. Operation must be commutative, associative, and idempotent.
  view - Render the crdt into a standard clojure type
  apply-op - Apply an operation for op-based crdts. Default is join."
  (join [a b])
  (view [c])
  (op [c o])
  (apply-op [c o]))

;; Default crdt methods
(extend-type #?(:clj Object :cljs js/Object)
  CRDT
  (op [c o] o)
  (apply-op [c o] (join c o)))

(defmulti ->crdt
  "Create a crdt type from edn representation"
  (fn [t & args] (keyword t)))


;;; Helper functions

(defn monoid
  "Forms a monoid of given crdt type for use with reducers"
  [type]
  #?(:clj (r/monoid join (constantly (->crdt type)))))

(defn fold
  "Applies a series of crdt operations in parallel"
  [type coll]
  #?(:clj (r/fold (monoid type) apply-op coll)
     :cljs (reduce apply-op (->crdt type) coll)))


;;; CRDT Types


;;; GSet - Grow-Only Set

(deftype GSet [values]
  CRDT
  (join [a b]
    (GSet. (union values (.values b))))
  (view [c] values)
  (apply-op [c op] (GSet. (conj values op))))

(defmethod ->crdt :gset
  ([_] (GSet. #{}))
  ([_ s] (GSet. s)))

; Convenience functions
(def gset (partial ->crdt :gset))
(def gset-add
  "Creates an operation that will add an element to a gset"
  identity)

#_
(view (fold :gset [1 2 3 4 5 6])) ;=> #{1 4 6 3 2 5}


;;; PNSet - Two Phase Set

(deftype PNSet [pos neg]
  CRDT
  (join [a b]
    (PNSet. (join pos (.pos b))
           (join neg (.neg b))))
  (view [c] (difference (view pos) (view neg)))
  (apply-op [c [op e]]
    (case op
      :add (PNSet. (apply-op pos e) neg)
      :rem (PNSet. pos (apply-op neg e)))))

(defmethod ->crdt :pnset
  ([_] (PNSet. (gset) (gset)))
  ([_ {:keys [pos neg]}] (PNSet. (gset pos) (gset neg))))

; Convenience functions
(def pnset (partial ->crdt :pnset))
(defn pnset-add [e] [:add e])
(defn pnset-rem [e] [:rem e])

#_
(view (fold :pnset [[:add 1]  [:add 2] [:add 3] [:add 4] [:rem 4]])) ;=> #{1 2 3}



(deftype GCounter [m]
  CRDT
  (join [a b]
    (->> (for [k (union (keys m) (keys (.m b)))
               :let [v #(get (.m %) k 0)]]
           [k (max (v a) (v b))])
      (into {})
      (GCounter.)))
  (view [c] (reduce + (vals (.m c))))
  (op [c [o node n]]
    (case o
      :set [node n]
      :inc [node (+ (get m node 0) n)]))
  (apply-op [c [node n]]
    (join c {node n})))

(defmethod ->crdt :gcounter
  ([_] (GCounter. {}))
  ([_ m] (GCounter. m)))

(defn gcounter-set
  "Produce op to set counter for node to value n"
  [node n] [node n])

(defn gcounter-inc
  "Produce op to increment counter value for local node"
  ([c node] (op c [:inc node 1]))
  ([c node n] (op c [:inc node n])))


(deftype PNCounter [pos neg]
  CRDT
  (join [a b]
    (PNCounter. (join pos (.pos b)) (join neg (.neg b))))
  (view [c] (- (view pos) (view neg)))
  (op [c [o node n]]
    (case o
      :inc [node (+ (get pos node 0) n)]
      :dec [node (+ (get neg node 0) n)]))
  (apply-op [c [op & op']]
    (case op
      :inc (PNCounter. (apply-op pos op') neg)
      :dec (PNCounter. pos (apply-op neg op')))))

(defmethod ->crdt :pncounter
  ([_] (->crdt :pncounter))
  ([_ {:keys [p n]}] (PNCounter. (->crdt :gcounter p) (->crdt :gcounter n))))

(defn pncounter-inc [c node n]
    ([c node] (pncounter-inc c node 1))
    ([c node n] [:inc node (->> c .p (get node 0) (+ n))]))

(defn pncounter-dec [c node n]
  ([c node] (pncounter-inc c node 1))
  ([c node n] [:dec node (->> c .n (get node 0) (+ n))]))

#_
(view
 (join
  (PNCounter.
   (GCounter. {:a 1 :b 2})
   (GCounter. {:a 3 :b 1}))
  (PNCounter.
   (GCounter. {:a 0 :b 3})
   (GCounter. {:b 1}))))

(deftype LwwRegister [time value]
  CRDT
  (join [c c'] (if (> time (.time c')) c c'))
  (view [c] value)
  (apply-op [c [t v]]
    (join c (LwwRegister. t v))))

(defmethod ->crdt :lww-register
  ([_] (LwwRegister. 0 0))
  ([_ {:keys [time value]}] (LwwRegister. time value)))

(defn lww-register-set [time value] [time value])

#_
(view (fold :lww-register [[2 :2] [4 :4] [1 :1]])) ;; => :4


;;; Unfinished
;; (defrecord Element [^Boolean add      ;; Operation: true = add, false = remove
;;                     ^Long timestamp]) ;; Epoch time (ms) of most recent update

;; (deftype LwwElementSet [s]
;;   CRDT
;;   (join [a b]
;;     (for [k (union (keys (.s a)) (keys (.s b)))
;;           :let [[x y] (map #(-> % .values (get k)) [a b])]]
;;       [k (match [x y]
;;            [nil nil] nil
;;            [nil _] y
;;            [_ nil] x
;;            :else (join x y))]))
;;   (view [c] (.value c)))
;; #_
;; (defmethod ->crdt :lww-element-set
;;   ([_] (LwwElementSet. {}))
;;   ([_ s] (LwwElementSet.
;;           (reduce-kv
;;            (fn [coll k v]

;;              ) s)
;;           )))

;; (def reservation-schema
;;   {:name "reservation"
;;    :type :record
;;    :fields [{:name :id, :type :string, :logicaltype :lww-register, :required true}
;;             {:name :rid, :type :int, :logicaltype :lww-register, :required true}
;;             {:name :partysize, :type :int, :logicaltype :lww-register, :required true}
;;             ;; {:name "scheduled", :type :long, :logicaltype :timestamp-millis, :required true}
;;             ;; {:name "state", :type :, :logicaltype :timestamp-millis, :required true}
;;             {:name :diner-name, :type :string, :logicaltype :lww-register, :required true}
;;             ]
;;    })

;; (deftype cRecord [schema values]
;;   CRDT
;;   (join [a b]
;;     (->>
;;         (for [{:keys [name logicaltype]} (:fields schema)
;;               :let [[x y] (map #(-> % .values (get name)) [a b])]]
;;           [name (match [x y]
;;                   [nil nil] nil
;;                   [nil _] y
;;                   [_ nil] x
;;                   :else (join x y))])
;;       (into {})
;;       (cRecord. schema)))
;;   (view [c]
;;     (->> (for [{:keys [name logicaltype]} (:fields schema)]
;;            [name (some-> c .values (get name) view)])
;;       (into {}))))

;; (defmethod ->crdt :crecord
;;   ([_ schema] (cRecord. schema {}))
;;   ([_ schema values]
;;    (->> (for [{:keys [name logicaltype]} (:fields schema)]
;;           [name (some->> name (get values) (->crdt logicaltype))])
;;      (filter #(-> % second some?))
;;      (into {})
;;      (cRecord. schema))))

;; (def a (->crdt :crecord reservation-schema
;;               {:id {:value 5 :time 1}}))

;; (def b (->crdt :crecord reservation-schema
;;                {:rid {:value 123 :time 2}}))

;; (view (join a b))

;; (def ops
;;   [
;;    {:entry 0
;;     :value {:id 1
;;             :rid 1
;;             :partysize 6}
;;     :time 1}
;;    {:entry 0
;;     :value {:id 1
;;             :rid 1
;;             :partysize 6}
;;     :time 1}
;;    ])
