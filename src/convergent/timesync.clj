(ns crdt.core.timesync
  "Syncs two sets of timestamped events
  . useful for anti-entropy of eventually consistent logs
  . employs a pruned hash trie of HULC timestamps
  . minimizes resending already synchronized events

  # Problem #
  Suppose we have two nodes, α and β.
  . α and β each have a list of HULC timestamped events, A and B
  . α and β wish to synchronize such that they both have A ∪ B
  . a simple method would be for α to send its complete list of events to β
  . unfortunately
    - the lists are long
    - the lists are not append-only (i.e. both lists having the same
      most recent event does not guarantee consistency)
  . fortunately
    - most events are already in both lists
    - older events are more likely to be in both lists

  # Solution #
  . α creates a hash-trie of A (see below)
  . let hashA be right edge pruned hash-trie of A
  . α sends hashA to β
  . β creates a hash-trie of it's list, let this be hashB
  . β compares hashA to hashB
  . if hashA == hashB, lists are in sync
  . else, (diff hashA hashB) returns the time of divergence τ
  . β sends α all events in B since time τ
  . α sends β all events in A since τ that are not already in B
  . profit???

  # Hash-Trie #
  . essentially a merkle tree keyed by epoch timestamp
  . timestamp is converted to a 8 digit hex string
  . top level of trie is all elements that share same first charactor
  . next level is second hex charactor, etc
  . this results in a 16-ary trie of height 8
  . each level contains a hash of all of its branches
  . pruning eliminates all but right-most branch of each level resulting in
    a linked list of length 8
  . pruned hash contains hashes separated from the current time by an
    exponentially increasing distance
  . by default: [8y 194d 12d 18h 4m 16s 1s]
  "
  (:require
   [clojure.string :as s]))

;; Some helper Functions

(def hash-fn hash)

(defn hash-cat [& hs] (mod (apply + hs) (Integer/MAX_VALUE)))

(defn hulc->ts [hulc] (-> hulc  (quot 1000)))

(defn hex->int [^String s] (Integer/parseInt s 16))

(defn zero-fill [t] (-> (format "%-8s" t) (s/replace \space \0)))

(def hexify (partial format "%08x"))

(def unhexify (comp hex->int zero-fill))

(def hex-keys (for [x (range 16)] (-> x hexify last)))

(defn key-union "union of keys from a seq of maps" [xs]
  (->> xs (apply merge) keys (filter char?) sort))

;; bucket size can be tuned by modifying hulc->keys and keys->ts
;; . hex-sec [8y 194d 12d 18h 4m 16s 1s]
;; . hex-ms [35y 2y 50d 3d 5h 17m 65s 4s 256ms 16ms 1ms]
;; . dec-ms [32y 3y 116d 12d 1d 3h 17m 2m 10s 1s 100ms 10ms 1ms]
;; . octal-ms [17y 2y 100d 12d 38h 5h 35m 4m 32s 4s 512ms 64ms 8ms 1ms]
;; . base4-ms [... 50d 12d 3d 19h 5h 1h 17m 4m 1m 16s 4s 1s 256ms 64ms 16ms 4ms 1ms]

(defn hulc->keys "Gets the trie-keys for a hulc based on its hex timestamp" [x]
  (-> x hulc->ts hexify (concat [x])))

(defn keys->ts "Get ms timestamp from trie-keys" [ks]
  (some-> ks not-empty s/join unhexify (* 1000)))

;; Trie Building

(defn trie-insert
  "Insert a hulc value x into a hash trie"
  [trie x]
  (let [h (hash-fn x)
        ks (hulc->keys x)
        e {:hash h}]
    (loop [t trie, i 0]
      (let [[k r] (split-at i ks)]
        (if (empty? r)
          (assoc-in t ks e)
          (recur (assoc-in t (concat k [:hash])
                           (hash-cat (get (get-in t k) :hash 0) h))
                 (inc i)))))))

(defn trie-build
  "Build a hash trie from a list of elements"
  [s]
  (reduce trie-insert {} s))

;; Trie Diffing

(defn hash-diff? "" [xs k]
  (apply not= (map #(-> % (get-in k) :hash) xs)))

(defn diff-keys [xs]
  (filter #(hash-diff? xs [%]) (key-union xs)))

(defn trie-diff
  "Determine if two tries are identical, and if not, when they diverge.
   Return nil if identical and ts otherwise."
  [a b]
  (if (not (hash-diff? [a b] []))
   nil
   (keys->ts
    (loop [xs [a b], k []]
      (if-let [next-k (first (diff-keys xs))]
        (recur (map #(get-in % [next-k]) xs), (conj k next-k))
        k)))))

;; Trie Pruning

(defn map-kv
    "map over a hashmap.
     f takes [k v] and outputs v"
    [f m]
    (into (empty m) (for [[k v] m] [k (f k v)])))

(defn prune
  "Keep only the greatest n keys in map"
  ([x] (prune 1 x))
  ([n x] (let [char-keys (->> x keys (filter char?) sort)]
           (apply dissoc x (drop-last n char-keys)))))

(defn trie-prune
  "Prune all hashes more than n-1 elements to the left of the current timestamp"
  ([trie] (trie-prune 2 trie))
  ([n trie]
   (->> trie
     (prune n)
     (map-kv (fn [k v]
               (if (and (char? k) (map? v))
                 (trie-prune n v)
                 v))))))

;; Alternative implementation, doesn't use map-kv
(defn trie-prune2
  "Prune all hashes more than n-1 elements to the left of the current timestamp"
  ([trie] (trie-prune 2 trie))
  ([n trie]
   (into {}
         (for [[k v] (prune n trie)]
           (if (and (char? k) (map? v))
             [k (trie-prune n v)]
             [k v])))))

(defn greatest-key [x]
  (->> x keys (filter char?) sort last))

(defn latest-ts
  "Rightmost branch of hash-trie"
  [trie]
  (loop [t trie ks []]
    (if-let [k (greatest-key t)]
      (recur (t k) (conj ks k))
      (keys->ts ks))))

(defn hash-snapshot
  "Create a snapshot hash vector"
  ([trie] (->> trie greatest-key (hash-snapshot trie)))
  ([trie ts]
   (loop [t trie, s [ts], h (trie :hash), ks (-> ts hexify seq)]
     (if-let [k (first ks)]
       (let [t* (t k), h* (:hash t*)]
         (recur t* (conj s (- h h*)) h* (rest ks)))
       (conj s h)))))

#_
(defn snapshot-diff
  "Compare a snapshot with a trie"
  [trie snapshot]
  (map #() (rest snapshot) (hash-snapshot trie (first snapshot)))
  (loop [k []
         ks (-> snapshot first hexify seq)
         h (second snapshot)
         hs (drop 2 snapshot)]
    (let [trie-hash (get-in trie (conj k :hash))
          snapshot-hash (last hs)
          comp []]
      (if (not=  (get-in trie (conj k :hash)) h)
        ()
        (recur (butlast ks) (butlast hs)))
      )))

#_
(go
  (let [resp (<! (>! input http-chan))
        resp (-> input
               (>! (http-chan)) <!
               )]))
