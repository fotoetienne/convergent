(ns convergent.crdt-test
  (:require [convergent.crdt :as c]
            [clojure.test :refer [deftest testing is]]))

;; TODO add generative tests for commutativity and idempotence

(defn commutative-join [xs]
  (= (reduce c/join xs)
     (reduce c/join (shuffle xs))))

(defn idempotent-join [xs]
  (= (reduce c/join xs)
     (reduce c/join (concat xs xs))))

(defn associative-join [xs]
  (= (reduce c/join xs)
     (reduce c/join (map c/join (partition 2 xs)))))

(defn commutative-ops [crdt ops]
  (= (c/fold crdt ops)
     (c/fold crdt (shuffle ops))))

(defn idempotent-ops [crdt ops]
  (= (c/fold crdt ops)
     (c/fold crdt (concat ops ops))))

(defn associative-ops [crdt ops]
  (= (c/fold crdt ops)
     (reduce c/join (map (partial c/fold crdt) (partition-all 5 ops)))))

(deftest gset-test
  (testing "fold over gset"
    (is (= #{1 2 3 4 5 6}
           (c/view (c/fold :gset [1 2 2 3 4 5 6 1]))))))

(deftest pnset-test
  (testing "fold over pnset ops"
    (is (= #{1 2 3}
           (c/view (c/fold :pnset [[:add 1] [:add 2] [:add 3] [:add 4] [:rem 4]]))))))

(deftest gcounter-test
  (testing "fold over gcounter ops"
    (is (= 3 (c/view (c/gcounter {:a 1 :b 2}))))
    (is (= 2 (c/view (c/effect (c/gcounter {:a 1}) [:a 2]))))
    (is (= 9 (c/view (c/fold :gcounter [[:a 1] [:a 2] [:b 3] [:c 4] [:a 1]]))))))

(deftest pncounter-test
  (testing "fold over pncounter ops"
    (is (= 8
           (c/view
            (c/fold :pncounter
                    [[:p :a 1] [:p :a 2] [:p :b 3] [:p :c 4] [:n :a 1]]))))))

(deftest lww-register-test
  (testing "fold over lww-register ops"
    (is (= :4 (c/view (c/fold :lww-register [[2 :2] [4 :4] [1 :1]]))))))
