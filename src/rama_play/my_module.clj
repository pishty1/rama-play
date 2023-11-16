(ns rama-play.my-module
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]
            [com.rpl.rama.test :as rtest]
            [com.rpl.rama :as rm]
            [com.rpl.rama.path :as rp]))

(defrecord MyRecord [key value])
(rm/defmodule MyModule [setup topologies]
  (rm/declare-depot setup *mydepot (rm/hash-by :key))
  (rm/declare-depot setup *mydepot2 (rm/hash-by :key))
  (let [s (rm/stream-topology topologies "s")]
    (rm/declare-pstate s $$mypstate {Long String})
    ;; (declare-pstate s $$mypstate2 (map-schema Long String))
    (rm/declare-pstate s $$mypstate2 {Long String})
    (rm/<<sources s
               (rm/source> *mydepot :> {:keys [*key *value] :as *input})
               (rm/local-transform> [(rp/keypath *key) (rp/termval *value)] $$mypstate)
               (rm/source> *mydepot2 :> {:keys [*key *value] :as *input})
               (rm/local-transform> [(rp/keypath *key) (rp/termval *value)] $$mypstate2))))


(comment
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc MyModule {:tasks 4 :threads 2})
    (let [mymodule (rm/get-module-name MyModule)
          mydepot (rm/foreign-depot ipc mymodule "*mydepot")
          mydepot2 (rm/foreign-depot ipc mymodule "*mydepot2")
          mypstate (rm/foreign-pstate ipc mymodule "$$mypstate")
          mypstate2 (rm/foreign-pstate ipc mymodule "$$mypstate2")
          _ (rm/foreign-append! mydepot (->MyRecord 1 "yeo"))
          _ (rm/foreign-append! mydepot (->MyRecord 2 "yeo2"))
          _ (rm/foreign-append! mydepot2 (->MyRecord 1 "Hello world"))
          myresult (rm/foreign-select-one (rp/keypath 2) mypstate)
          myresult2 (rm/foreign-select-one (rp/keypath 1) mypstate2)]
      (println "from depot 1 " myresult)
      (println "from depot 2 " myresult2)))

  )