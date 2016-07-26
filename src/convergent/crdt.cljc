(ns convergent.crdt
  (:require
   [clojure.set :refer [union difference]]
   [clojure.core.match :refer [match]]
   #?(:clj [clojure.core.reducers :as r])))


;;; Protocol

(defprotocol CRDT
  "CRDT protocol.

  This protocol defines a Convergent Replicated Data Type

  join - Necessary to define a join-semilattice. Joins two crdts of the same
         type. Operation must be commutative, associative, and idempotent.
  view - Render the crdt into a standard clojure type
  apply-op - Apply an operation for op-based crdts. Default implementation is
             to join."
  (join [a b])
  (view [c])
  (apply-op [c op]))

(extend Object CRDT {:join merge, :view identity, :apply-op join})

(defmulti ->crdt
  "Create a crdt type from edn representation"
  (fn [t & xs] (keyword t)))


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


(deftype GSet [values]
  CRDT
  (join [a b]
    (GSet. (union values (.values b))))
  (view [c] values)
  (apply-op [c op] (GSet. (conj values op))))

(defmethod ->crdt :gset
  ([_] (GSet. #{}))
  ([_ s] (GSet. s)))

(def gset-add
  "Creates an operation that will add an element to a gset"
  identity)

#_
(view (fold :gset [1 2 3 4 5 6]))


(deftype PNSet [p n]
  CRDT
  (join [a b]
    (PNSet. (union p (.p b))
           (union n (.n b))))
  (view [c] (difference p n))
  (apply-op [c [op e]]
    (case op
      :add (PNSet. (conj p e) n)
      :rem (PNSet. p (conj n e)))))

(defmethod ->crdt :pnset
  ([_] (->crdt :pnset #{} #{}))
  ([_ {:keys [p n]}] (PNSet. (set p) (set n))))

(defn pnset-add [e] [:add e])
(defn pnset-rem [e] [:rem e])


(deftype GCounter [m]
  CRDT
  (join [a b]
    (->> (for [k (union (keys (.m a)) (keys (.m b)))
               :let [v #(get (.m %) k 0)]]
           [k (max (v a) (v b))])
      (into {})
      (GCounter.)))
  (view [c] (reduce + (vals (.m c))))
  (apply-op [c [node n]]
    (join c )
    ))

(defmethod ->crdt :pnset
  ([_] (GCounter. {}))
  ([_ m] (GCounter. m)))

(defn gcounter-set
  "Produce op to set counter for node to value n"
  [node n] [node n])

(defn gcounter-inc
  "Produce op to increment counter value for local node"
  ([c node] (gcounter-inc c node 1))
  ([c node n] [node (->> c .m (get node 0) (+ n))]))


(deftype PNCounter [p n]
  CRDT
  (join [a b]
    (PNCounter. (join p (.p b)) (join n (.n b))))
  (view [c] (- (view p) (view n)))
  (apply-op [c [op & op']]
    (case op
      :inc (PNCounter. (apply-op p op') n)
      :dec (PNCounter. p (apply-op n op')))))

(defmethod ->crdt :pncounter
  ([_] (->crdt :pncounter))
  ([_ {:keys [p n]}] (PNCounter. (->crdt :gcounter p) (->crdt :gcounter n))))

#_
(view
 (join
  (PNCounter.
   (GCounter. {:a 1 :b 2})
   (GCounter. {:a 3 :b 1}))
  (PNCounter.
   (GCounter. {:a 0 :b 3})
   (GCounter. {:b 1}))))

(deftype LwwElem [time value]
  CRDT
  (join [a b]
    (if (> (.time a) (.time b))
      a
      b))
  (view [c] (.value c)))

(defmethod ->crdt :lww-elem
  ([_] (LwwElem. 0 0))
  ([_ {:keys [time value]}] (LwwElem. time value)))

(view (->crdt :lww-elem {:time 0 :value 0}))

(def a (->crdt :lww-elem))
(def ops [{:time 3 :value 1} {:time 4 :value 1}])

(defn apply-ops [ops] (reduce join ops))

(->> ops
  (map (partial ->crdt :lww-elem))
  (reduce join)
  view)

(defrecord Element [^Boolean add      ;; Operation: true = add, false = remove
                    ^Long timestamp]) ;; Epoch time (ms) of most recent update

(deftype LwwElementSet [s]
  CRDT
  (join [a b]
    (for [k (union (keys (.s a)) (keys (.s b)))
          :let [[x y] (map #(-> % .values (get k)) [a b])]]
      [k (match [x y]
           [nil nil] nil
           [nil _] y
           [_ nil] x
           :else (join x y))]))
  (view [c] (.value c)))
#_
(defmethod ->crdt :lww-element-set
  ([_] (LwwElementSet. {}))
  ([_ s] (LwwElementSet.
          (reduce-kv
           (fn [coll k v]

             ) s)
          )))

(def reservation-schema
  {:name "reservation"
   :type :record
   :fields [{:name :id, :type :string, :logicaltype :lww-elem, :required true}
            {:name :rid, :type :int, :logicaltype :lww-elem, :required true}
            {:name :partysize, :type :int, :logicaltype :lww-elem, :required true}
            ;; {:name "scheduled", :type :long, :logicaltype :timestamp-millis, :required true}
            ;; {:name "state", :type :, :logicaltype :timestamp-millis, :required true}
            {:name :diner-name, :type :string, :logicaltype :lww-elem, :required true}
            ]
   })

(deftype cRecord [schema values]
  CRDT
  (join [a b]
    (->>
        (for [{:keys [name logicaltype]} (:fields schema)
              :let [[x y] (map #(-> % .values (get name)) [a b])]]
          [name (match [x y]
                  [nil nil] nil
                  [nil _] y
                  [_ nil] x
                  :else (join x y))])
      (into {})
      (cRecord. schema)))
  (view [c]
    (->> (for [{:keys [name logicaltype]} (:fields schema)]
           [name (some-> c .values (get name) view)])
      (into {}))))

(defmethod ->crdt :crecord
  ([_ schema] (cRecord. schema {}))
  ([_ schema values]
   (->> (for [{:keys [name logicaltype]} (:fields schema)]
          [name (some->> name (get values) (->crdt logicaltype))])
     (filter #(-> % second some?))
     (into {})
     (cRecord. schema))))

(def a (->crdt :crecord reservation-schema
              {:id {:value 5 :time 1}}))
(def b (->crdt :crecord reservation-schema
               {:rid {:value 123 :time 2}}))
(view (join a b))

(def ops
  [
   {:entry 0
    :value {:id 1
            :rid 1
            :partysize 6}
    :time 1}
   {:entry 0
    :value {:id 1
            :rid 1
            :partysize 6}
    :time 1}
   ])
