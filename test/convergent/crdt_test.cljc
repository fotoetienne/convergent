(ns convergent.crdt-test
  (:require [convergent.crdt :as c]
            #?(:clj  [clojure.test :refer :all]
               :cljs [cljs.test :refer-macros [deftest testing is]])))

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
    (is (= 2 (c/view (c/apply-op (c/gcounter {:a 1}) [:a 2]))))
    (is (= 9 (c/view (c/fold :gcounter [[:a 1] [:a 2] [:b 3] [:c 4] [:a 1]]))))))

(deftest pncounter-test
  (testing "fold over pncounter ops"
    (is (= 8
           (c/view
            (c/fold :pncounter
                    [[:p :a 1] [:p :a 2] [:p :b 3] [:p :c 4] [:n :a 1]]))))))

;; TODO add generative tests for commutativity and idempotence
