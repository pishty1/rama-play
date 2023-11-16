(ns rama-clojure-starter.my-module
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]
            [com.rpl.rama.test :as rtest]))

(defrecord MyRecord [key value])
(defmodule MyModule [setup topologies]
  (declare-depot setup *mydepot (hash-by :key))
  (declare-depot setup *mydepot2 (hash-by :key))
  (let [s (stream-topology topologies "s")]
    (declare-pstate s $$mypstate {Long String})
    ;; (declare-pstate s $$mypstate2 (map-schema Long String))
    (declare-pstate s $$mypstate2 {Long String})
    (<<sources s
               (source> *mydepot :> {:keys [*key *value] :as *input})
               (println "depot 1 adding " *key " and value " *value)
               (local-transform> [(keypath *key) (termval *value)] $$mypstate)
               (source> *mydepot2 :> {:keys [*key *value] :as *input})
               (println "depot 2 adding " *key " and value " *value)
               (local-transform> [(keypath *key) (termval *value)] $$mypstate2))))


(comment
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc MyModule {:tasks 4 :threads 2})
    (let [mymodule (get-module-name MyModule)
          mydepot (foreign-depot ipc mymodule "*mydepot")
          mydepot2 (foreign-depot ipc mymodule "*mydepot2")
          mypstate (foreign-pstate ipc mymodule "$$mypstate")
          mypstate2 (foreign-pstate ipc mymodule "$$mypstate2")
          _ (foreign-append! mydepot (->MyRecord 1 "yeo"))
          _ (foreign-append! mydepot2 (->MyRecord 1 "Hello world"))
          myresult (foreign-select-one (keypath 1) mypstate)
          myresult2 (foreign-select-one (keypath 1) mypstate2)]
      (println "from depot 1 " myresult)
      (println "from depot 2 " myresult2)))
  (def myrange (range 0 10))
  (map (fn [x] (println "hello " x)) myrange)

  )