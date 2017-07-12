(ns convergent.util)

(defn map-v
  "Map f over values of a hashmap"
  [f coll]
  (into {} (for [[k v] coll] [k (f v)])))

(defn filter-kv [f m] (into {} (filter f m)))
