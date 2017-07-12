(ns convergent.crdt
  (:require
   [clojure.set :refer [union difference]]
   [convergent.util :refer [map-v]]
   [clojure.spec.alpha :as s]
   #?(:clj [clojure.core.reducers :as r])))


;;; Protocol

(defprotocol Convergent
  "CRDT protocol.

  This protocol defines a Convergent Replicated Data Type

  join - Necessary to define a join-semilattice. Joins two crdts of the same
         type. Operation must be commutative, associative, and idempotent.
  view - Render the crdt into a standard clojure type"
  (join [c c'])
  (view [c]))

(defprotocol Operational
  "Operation based CRDT

  prepare - Create an operation to be applied later.
  effect - Apply an operation for op-based crdts. Default is join."
  (prepare [c o])
  (effect [c o]))

(defprotocol Serializable
  "State can be output as pure edn
  This edn should result in an identical object when passed to ->crdt
  with appropriate type tag."
  (crdt-type [c])
  (marshal [c]))

(defmulti ->crdt
  "Create a crdt type from edn representation"
  (fn [t & args] (keyword (name t))))


;;; Helper functions

(defn monoid
  "Forms a monoid of given crdt type for use with reducers"
  [crdt-type]
  #?(:clj (r/monoid join (constantly (->crdt crdt-type)))))

(defn fold
  "Applies a series of crdt operations in parallel"
  [crdt-type coll]
  #?(:clj (r/fold (monoid crdt-type) effect coll)
     :cljs (reduce effect (->crdt crdt-type) coll)))

;; NaN fails equality with itself and thus fails commutativity tests
(defn nan? [x] (and (number? x) (not= x x)))
(s/def ::any (s/and any? (complement nan?)))
(s/def ::some (s/and ::any some?))
(s/def ::number (s/and number? ::any))
(s/def ::int (s/and int? ::any))


;;; # CRDT Types #


;;; GSet - Grow-Only Set
(deftype GSet [s]
  Convergent
  (join [_ c'] (GSet. (union s (.-s c'))))
  (view [_] s)
  Operational
  (prepare [c o] o)
  (effect [_ op] (GSet. (conj s op)))
  Serializable
  (crdt-type [_] :gset)
  (marshal [_] (vec s)))

(s/def ::gset (s/coll-of ::some))
(s/def ::gset-op ::some)

(defmethod ->crdt :gset
  ([_] (GSet. #{}))
  ([_ s] (GSet. (set s))))

; Convenience functions
(def gset (partial ->crdt :gset))
(def gset-add
  "Creates an operation that will add an element to a gset"
  identity)

#_
(view (fold :gset [1 2 3 4 5 6])) ;=> #{1 4 6 3 2 5}


;;; PNSet - Two Phase Set

(deftype PNSet [pos neg]
  Convergent
  (join [c c']
    (PNSet. (join pos (.-pos c'))
           (join neg (.-neg c'))))
  (view [c] (difference (view pos) (view neg)))
  Operational
  (prepare [c o] o)
  (effect [c [op e]]
    (case op
      :add (PNSet. (effect pos e) neg)
      :rem (PNSet. pos (effect neg e))))
  Serializable
  (crdt-type [c] :pnset)
  (marshal [c] [(marshal pos) (marshal neg)]))

(s/def ::pos ::gset)
(s/def ::neg ::gset)
(s/def ::pnset (s/cat :pos ::pos :neg ::neg))
(s/def ::pnset-op (s/cat :op #{:add :rem}
                         :value ::gset-op))

(defmethod ->crdt :pnset
  ([_] (PNSet. (gset) (gset)))
  ([_ [pos neg]] (PNSet. (gset pos) (gset neg))))

; Convenience functions
(def pnset (partial ->crdt :pnset))
(defn pnset-add [e] [:add e])
(defn pnset-rem [e] [:rem e])

#_
(view (fold :pnset [[:add 1]  [:add 2] [:add 3] [:add 4] [:rem 4]])) ;=> #{1 2 3}



(deftype GCounter [m]
  Convergent
  (join [c c'] (GCounter. (merge-with max m (.-m c'))))
  (view [c] (reduce + (vals m)))
  Operational
  (prepare [c [o node n]]
    (case o
      :set [node n]
      :inc [node (+ (get m node 0) n)]))
  (effect [c [node n]]
    (join c (GCounter. {node n})))
  Serializable
  (crdt-type [_] :gcounter)
  (marshal [_] m))

(s/def :gcounter/node ::some)
(s/def :gcounter/count pos-int?)
(s/def ::gcounter (s/map-of :gcounter/node :gcounter/count))
(s/def ::gcounter-op (s/cat :node :gcounter/node
                            :count :gcounter/count))

(defmethod ->crdt :gcounter
  ([_] (GCounter. {}))
  ([_ m] (GCounter. m)))

;; Convenience functions
(def gcounter (partial ->crdt :gcounter))

(defn gcounter-set
  "Produce op to set counter for node to value n"
  [node n] [node n])

(defn gcounter-inc
  "Produce op to increment counter value for local node"
  ([c node] (prepare c [:inc node 1]))
  ([c node n] (prepare c [:inc node n])))

#_
(view (effect (gcounter {:a 1 :b 2}) [:a 1])) ; => 3
#_
(view (fold :gcounter [[:a 1] [:a 2] [:b 3] [:c 4] [:a 1]])) ; => 9


(deftype PNCounter [pos neg]
  Convergent
  (join [c c']
    (PNCounter. (join pos (.-pos c')) (join neg (.-neg c'))))
  (view [c] (- (view pos) (view neg)))
  Operational
  (prepare [c [o node n]]
    (case o
      :inc [node :p (+ (get pos node 0) n)]
      :dec [node :n (+ (get neg node 0) n)]))
  (effect [c [op & op']]
    (case op
      :p (PNCounter. (effect pos op') neg)
      :n (PNCounter. pos (effect neg op')))))

(s/def ::p ::gcounter)
(s/def ::n ::gcounter)
(s/def ::pncounter (s/keys :req-un [::p ::n]))
(s/def ::pncounter-op (s/cat :op #{:p :n}
                             :node :gcounter/node
                             :count :gcounter/count))

(defmethod ->crdt :pncounter
  ([_] (PNCounter. (gcounter) (gcounter)))
  ([_ {:keys [p n]}] (PNCounter. (gcounter p) (gcounter n))))

;; Convenience functions
(def pncounter (partial ->crdt :pncounter))

(defn pncounter-inc
  ([c node] (pncounter-inc c node 1))
  ([c node n] (prepare c [:inc node n])))

(defn pncounter-dec
  ([c node] (pncounter-dec c node 1))
  ([c node n] (prepare c [:dec node n])))

#_
(view (effect (pncounter {:p {:a 2}}) [:p :a 3])) ; => 3


(deftype LwwRegister [time value]
  Convergent
  (join [c c']
    (let [x (compare [time (str value)] [(.-time c') (str (.-value c'))])]
      (if (>= x 0) c c')))
  (view [c] value)
  Operational
  (prepare [c o] o)
  (effect [c [t v]]
    (join c (LwwRegister. t v)))
  Serializable
  (marshal [c] {:time time :value value}))

(s/def :lww-register/time int?)
(s/def :lww-register/value ::any)
(s/def ::lww-register
  (s/keys :req-un [:lww-register/time :lww-register/value]))
(s/def ::lww-register-op (s/cat :time :lww-register/time
                                :value :lww-register/value))

(defmethod ->crdt :lww-register
  ([_] (LwwRegister. 0 0))
  ([_ {:keys [time value]}] (LwwRegister. time value)))

;; Convenience functions
(def lww-register (partial ->crdt :lww-register))
(defn lww-register-set [time value] [time value])

#_
(view (fold :lww-register [[2 :2] [4 :4] [1 :1] [5 :5] [5 :1]])) ;; => :5


;; Const
; Constant type.

(deftype Const [v]
  Convergent
  (join [c c'] (Const. (or v (.-v c'))))
  (view [c] v)
  Operational
  (prepare [c o] o)
  (effect [c op] (join c (Const. op)))
  Serializable
  (crdt-type [_] :const)
  (marshal [_] v))

(s/def ::const ::any)
(s/def ::const-op ::any)

(defmethod ->crdt :const
  ([_] (Const. nil))
  ([_ v] (Const. v)))

(def const (partial ->crdt :const))

#_
(view (fold :const [nil 1 2 3])) ; => 1


;; GMap - Grow-only map

(deftype GMap [m]
  Convergent
  (join [c c'] (GMap. (merge-with join m (.-m c'))))
  (view [c] (map-v view m))
  Operational
  (prepare [c o] o)
  ;; (effect [c [k v]] (join c (GMap. {k v})))
  (effect [c [k v]]
    (join c (GMap. {k (if (get c k) (effect (get c k) v) (effect (GMap. {}) v))}))))

(s/def ::gmap (s/keys))
(s/def ::gmap-op (s/cat :key ::some :value ::some))

(defmethod ->crdt :gmap
  ([_] (GMap. {}))
  ([_ type m] (GMap. (map-v (partial ->crdt type) m))))

(def gmap (partial ->crdt :gmap))

(defn gmap-assoc [c k v]
  (prepare :gmap [k v]))


;; LwwElementSet

(deftype LwwElementSet [m]
  Convergent
  (join [c c'] (LwwElementSet. (merge-with join m (.-m c'))))
  (view [c] (->> m
              (filter (fn [[k v]] (not= :rem (view v))))
              keys
              (into #{})))
  Operational
  (prepare [c o] o)
  (effect [c [k t v]] (join c (LwwElementSet. {k (LwwRegister. t v)})))
  Serializable
  (marshal [_] (map-v marshal m)))

(defmethod ->crdt :lww-element-set
  ([_] (LwwElementSet. {}))
  ([_ m] (LwwElementSet. (map-v (partial ->crdt :lww-register) m))))

(def lww-element-set (partial ->crdt :lww-element-set))

#_
(def a (lww-element-set {:a {:time 0 :value :add} :b {:time 0 :value :rem}}))
#_
(view (join a (lww-element-set {:c {:time 2 :value :add}})))
#_
(marshal a)


;; cRecord

(deftype cRecord [schema values]
  Convergent
  (join [a b]
    (if (and a b)
      (->>
          (for [{:keys [name logicaltype]} (:fields schema)
                :let [[x y] (map #(-> % .-values (get name)) [a b])]]
            [name (or (and x y (join x y)) x y)])
        (into {})
        (cRecord. schema))
      (or a b)))
  (view [c]
    (->> (for [{:keys [name logicaltype]} (:fields schema)]
           [name (some-> c .-values (get name) view)])
      (into {})))
  Operational
  (prepare [c o] o)
  (effect [c [k v]] (cRecord. schema (update values k effect v)))
  Serializable
  (crdt-type [_] :crecord)
  (marshal [_] (map-v marshal values)))

(defmethod ->crdt :crecord
  ([_ schema] (cRecord. schema {}))
  ([_ schema values]
   (->> (for [{:keys [name logicaltype]} (:fields schema)]
          [name (->> name (get values) (->crdt logicaltype))])
     (filter #(-> % second some?))
     (into {})
     (cRecord. schema))))


;; HOURMap - Homogeneous Observed Update Remove Map
(deftype HOURMap [base m removed]
  Convergent
  (join [c c'] (HOURMap. base
                        (merge-with join m (.-m c'))
                        (union removed (.-removed c'))))
  (view [c] (into {} (for [[k v] m
                           :when (not (removed k))]
                       [k (view v)])))
  Operational
  (prepare [c o] o)
  (effect [c [cmd k v]]
    (case cmd
      :create
      (join c (HOURMap. base {k v} removed))
      :update
      (join c (HOURMap. base {k (effect (get c k (->crdt base)) v)} removed))
      :delete
      (HOURMap. base m (conj removed k)))))

  (defmethod ->crdt :hourmap
    ([_ base] (HOURMap. base {} #{}))
    ([_ base m] (HOURMap. base (map-v base m) #{})))

  (def hourmap (partial ->crdt :hourmap))


;; LwwMap

(deftype LwwMap [m]
  Convergent
  (join [c c'] (LwwMap. (merge-with join m (.-m c'))))
  (view [c] (->> m
              (map-v view)
              (filter (fn [[k v]] (not= ::rem v)))
              (into {})))
  Operational
  (prepare [c o] o)
  (effect [c [k t v]] (join c (LwwMap. {k (LwwRegister. t v)})))
  Serializable
  (marshal [_] (map-v marshal m)))

(s/def ::lww-map (s/map-of ::some ::lww-register))
(s/def ::lww-map-op (s/cat :key ::some
                           :time :lww-register/time
                           :value :lww-register/value))

(defmethod ->crdt :lww-map
  ([_] (LwwMap. {}))
  ([_ m] (LwwMap. (map-v (partial ->crdt :lww-register) m))))

(def lww-map (partial ->crdt :lww-map))

(defn lww-map-delete
  "Remove an itm from a lww-map"
  [c k time] (effect c [k time ::rem]))

#_
(def a (lww-map {:a {:time 0 :value :aaa} :b {:time 0 :value ::rem}}))
#_
(view (join a (lww-map {:c {:time 2 :value :ccc}})))
#_
(view (lww-map (marshal a)))



;; Or
; Boolean Or type.

(deftype Or [v]
  Convergent
  (join [c c'] (Or. (or v (.-v c'))))
  (view [c] v)
  Operational
  (prepare [c o] o)
  (effect [c op] (join c (Or. op)))
  Serializable
  (crdt-type [_] :or)
  (marshal [_] v))

(s/def ::or boolean?)
(s/def ::or-op boolean?)

(defmethod ->crdt :or
  ([_] (Or. nil))
  ([_ v] (Or. v)))

#_
(view (fold :or [false false false])) ; => false
#_
(view (fold :or [false true false])) ; => true

