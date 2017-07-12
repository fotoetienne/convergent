(ns convergent.runner
  "A stub namespace to run cljs tests using doo"

(:require [doo.runner :refer-macros [doo-tests]]
          [convergent.crdt-test]
          [convergent.spec-test]
          [convergent.timesync-test]))

(doo-tests 'convergent.crdt-test
           'convergent.spec-test
           'convergent.timesync-test)
