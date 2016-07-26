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
           (c/view (c/fold :pnset [[:add 1]  [:add 2] [:add 3] [:add 4] [:rem 4]]))))))
