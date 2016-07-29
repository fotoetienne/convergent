(ns convergent.spec
  (:require
   #?(:clj [clojure.spec :as s]
      :cljs [cljs.spec :as s])))

;; GSet
(s/def ::gset set?)
(s/def ::gset-op any?)

;; PNSet
(s/def ::pos ::gset)
(s/def ::neg ::gset)
(s/def ::pnset (s/keys :req-un [:convergent.spec/pos :convergent.spec/neg]))
(s/def ::pnset-op (s/cat :op #{:add :rem}
                         :value ::gset-op))

(s/conform ::pnset {:p #{} :n #{}})

;; GCounter
(s/def ::gcounter (s/keys))
(s/def ::gcounter-op (s/cat :node some?
                            :count number?))
;; PNCounter
(s/def ::p ::gcounter)
(s/def ::n ::gcounter)
(s/def ::pncounter (s/keys :req-un [:convergent.spec/p :convergent.spec/n]))
(s/def ::pncounter-op (s/cat :op #{:inc :dec}
                             :node some?
                             :count number?))

;; LwwRegister
(s/def ::lww-register
  (s/keys :req-un [:convergent.spec/time :convergent.spec/value]))
(s/def ::lww-register-op (s/cat :time some?
                                :value any?))

(s/explain-data ::pnset-op [:add 2])
(s/conform ::gset-op 1)
