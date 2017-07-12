(ns convergent.spec-test
  (:require
   [convergent.crdt :as c :refer [join view ->crdt fold]]
   [clojure.spec.alpha :as spec]
   [clojure.spec.gen.alpha :as gen]
   [clojure.test :refer [testing is]]
   [clojure.test.check :as tc]
   [clojure.test.check.clojure-test :refer [defspec] :include-macros true]
   [clojure.test.check.properties :as prop :include-macros true]))

(defn crdt-op-spec
  "Return the op spec corresponding to the given crdt spec"
  [crdt-spec]
  (keyword (namespace crdt-spec) (str (name crdt-spec) "-op")))

(defn join-test [crdt-spec]
  (prop/for-all [xs (-> crdt-spec spec/gen (gen/vector 10))]
                (let [crdts (map (partial ->crdt crdt-spec) xs)]
                  (= (view (reduce join crdts))
                     ;; commutative
                     (view (reduce join (shuffle crdts)))
                     ;; idempotent
                     (view (reduce join (concat crdts crdts)))
                     ;; associative
                     (view (reduce join
                                   (map (partial apply join)
                                        (partition 2 crdts))))))))

(defn ops-test [crdt-spec]
  (prop/for-all [ops (-> crdt-spec crdt-op-spec spec/gen (gen/vector 10))]
                (= (view (fold crdt-spec ops))
                   ;; commutative
                   (view (fold crdt-spec (shuffle ops)))
                   ;; idempotent
                   (view (fold crdt-spec (concat ops ops)))
                   ;; associative
                   (view (reduce join
                                 (map (partial fold crdt-spec)
                                      (partition-all 5 ops)))))))

(defspec gset-join-test 10
  (join-test :convergent.crdt/gset))

(defspec gset-ops-test 10
  (ops-test :convergent.crdt/gset))

(defspec pnset-join-test 10
  (join-test :convergent.crdt/pnset))

(defspec pnset-ops-test 10
  (ops-test :convergent.crdt/pnset))

(defspec gcounter-join-test 10
  (join-test :convergent.crdt/gcounter))

(defspec gcounter-ops-test 10
  (ops-test :convergent.crdt/gcounter))

(defspec pncounter-join-test 10
  (join-test :convergent.crdt/pncounter))

(defspec pncounter-ops-test 10
  (ops-test :convergent.crdt/pncounter))

(defspec lww-register-join-test 10
  (join-test :convergent.crdt/lww-register))

(defspec lww-register-ops-test 10
  (ops-test :convergent.crdt/lww-register))

(defspec lww-map-join-test 10
  (join-test :convergent.crdt/lww-map))

(defspec lww-map-ops-test 10
  (ops-test :convergent.crdt/lww-map))

(defspec or-join-test 10
  (join-test :convergent.crdt/or))

(defspec or-ops-test 10
  (ops-test :convergent.crdt/or))
