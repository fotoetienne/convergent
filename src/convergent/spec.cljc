(ns convergent.spec
  (:require
   [clojure.spec.alpha :as s]))

(s/def ::some (s/and any? some?))

;; GSet
(s/def ::gset (s/coll-of ::some))
(s/def ::gset-op ::some)

;; PNSet
(s/def ::pos ::gset)
(s/def ::neg ::gset)
(s/def ::pnset (s/cat :pos ::pos :neg ::neg))
(s/def ::pnset-op (s/cat :op #{:add :rem}
                         :value ::gset-op))

(s/conform ::pnset [#{} #{}])

;; GCounter
(s/def ::gcounter (s/map-of any? int?))
(s/def ::gcounter-op (s/cat :node ::some
                            :count number?))
;; PNCounter
(s/def ::p ::gcounter)
(s/def ::n ::gcounter)
(s/def ::pncounter (s/keys :req-un [:convergent.spec/p :convergent.spec/n]))
(s/def ::pncounter-op (s/cat :op #{:inc :dec}
                             :node ::some
                             :count number?))

;; LwwRegister
(s/def ::lww-register
  (s/keys :req-un [:convergent.spec/time :convergent.spec/value]))
(s/def ::lww-register-op (s/cat :time ::some
                                :value any?))

(s/explain-data ::pnset-op [:add 2])
(s/conform ::gset-op 1)
