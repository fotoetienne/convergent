(ns convergent.timesync-test
  (:require [convergent.timesync :as t :refer [hex->int zero-fill hexify
                                               unhexify key-union hex-keys
                                               trie-build hash-cat trie-prune
                                               prune trie-diff greatest-key
                                               latest-ts hash-snapshot hash-fn]]
            #?(:clj  [clojure.test :refer :all]
               :cljs [cljs.test :refer-macros [deftest testing is]])))

(def now #?(:clj (System/currentTimeMillis)
            :cljs (.getTime (js/Date.))))

(defn exp [x n] (reduce * (repeat n x)))

(defn- make-clock [s]
  (let [a (atom s)]
    (fn []
      (let [c @a
            t (first c)]
        (if (compare-and-set! a c (next c))
          t
          (recur))))))

(deftest hex->int-test
  (is (= (hex->int "0000000f")
         15))
  (is (= (hex->int "000000ff")
         255))
  (is (= (hex->int "f00")
         (* 16 16 15))))

(deftest zero-fill-test
  (is (= "f0000000"
         (zero-fill "f")))
  (is (= "0000000f"
         (zero-fill "0000000f"))))

(deftest hexify-test
  (is (= (hexify 15)
         "0000000f"))
  (is (= (hexify 255)
         "000000ff")))

(deftest unhexify-test
  (is (= (unhexify "1")
         (exp 16 7)))
  (is (= (unhexify "00f")
         (* (exp 16 5) 15))))

(deftest key-union-test
  (is (= [\0 \1 \2 \f]
         (key-union [{\0 \0 \1 \1 \f \f} {\1 \1 \2 \2}]))))

(deftest hex-keys-test
  (is (= '(\0 \0 \0 \0 \0 \0 \f \f)
         (drop-last (hex-keys 255)))))

(deftest trie-build-test
  (is (let [tss (map #(- now (rand-int (exp 2 %))) (range 5))]
        (trie-build hex-keys tss))))

(deftest trie-build-static-test
  (is
   (let [data [15 255 4095]
         h (apply hash-cat (map hash-fn data))]
     (= {\0 {\0 {\0 {\0 {\0
                         {\f {\f {\f {4095 {:hash (hash-fn 4095)},
                                      :hash (hash-fn 4095)},
                                  :hash (hash-fn 4095)},
                              :hash (hash-fn 4095)},
                          \0 {\f {\f {255 {:hash (hash-fn 255)},
                                      :hash (hash-cat (hash-fn 255))},
                                  :hash (hash-cat (hash-fn 255))},
                              \0 {\f {15 {:hash (hash-fn 15)},
                                      :hash (hash-fn 15)},
                                  :hash (hash-fn 15)},
                              :hash (apply hash-cat (map hash-fn [15 255]))},
                          :hash h},
                         :hash h},:hash h}, :hash h}, :hash h}, :hash h}
        (trie-build hex-keys data)))))

(def h (apply hash-cat (map hash [15 255 4095])))
(= h (-> (trie-build hex-keys [15 255 4095])
       (get-in [\0 \0 \0 \0 \0 \0])
       :hash))

(def test-data (->> (range 100)
                    (map #(- now (* (rand-int (exp % 2)) 1000)))))
(deftest commutivity-test
  (let [data test-data
        [t1 t2] (map (partial trie-build hex-keys) [data (shuffle data)])
        [p1 p2] (map trie-prune [t1 t2])]
    (is (= t1 t2))
    (is (= p1 p2))))

(deftest prune-test
  (is (= (prune {\1 \1 \2 \2 \3 \3 \e \e})
         {\e \e}))
  (is (= (prune 2 {\4 \4 \1 \1 \2 \2 \3 \3 \e \e})
         {\4 \4 \e \e})))

(deftest trie-prune-test
  (let [t {\1 \1 \2 \2 \3 \3 \e {\1 \1 \2 {\3 {\4 \4}}}}]
    (is (= {\e {\2 {\3 {\4 \4}}}}
           (trie-prune 1 t)))
    (is (= {\3 \3, \e {\1 \1, \2 {\3 {\4 \4}}}}
           (trie-prune t)))))

(deftest trie-diff-test
  (let [divergence (-> now (- 1e4) (quot 1e3) long)
        d1 (shuffle test-data)
        d2 (conj (shuffle test-data) divergence)
        [t1 t2] (map (partial trie-build hex-keys) [d1 d2])]
    (is (nil? (trie-diff t1 t1)))
    (is (= divergence (trie-diff t1 t2)))))

(deftest greatest-key-test
  (is (= (greatest-key {\1 1 \2 1 :hash 1 \e 1 \3 1})
         \e)))

(def simple-trie
  {:hash 9,
   \1 {:hash 8,
       \2 {:hash 2,
           \f {:hash 3}}
       \e {:hash 7,
           \3 {:hash 6,
               \7 {:hash 5,
                   \0 {:hash 4,
                       \0 {:hash 3,
                           \0 {:hash 2,
                               \0 {:hash 1}}}}}}}},
   \0 {:hash 1}})

(deftest latest-ts-test
  (is (= (latest-ts simple-trie)
         (unhexify "1e370000"))))

(deftest hash-snapshot-test
  (let [ts (unhexify "1e370000")]
    (is (= (hash-snapshot simple-trie ts)
           [ts 1 1 1 1 1 1 1 1 1]))))
