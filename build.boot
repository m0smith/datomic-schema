
(set-env!
 #_:repositories  #_[["clojars" {:url "https://clojars.org/repo/"
                             :username (System/getenv "CLOJARS_USER")
                             :password (System/getenv "CLOJARS_PASS")}]]
 :resource-paths #{"resources"}
 :source-paths   #{"src"}
 :dependencies   '[#_[org.clojure/core.async "0.5.527"     :scope "provided"
                    :exclusions [org.clojure/clojure]]
                   [org.onyxplatform/onyx-datomic  "0.14.5.0"
                    :exclusions [org.onyxplatform/onyx org.clojure/clojure]] ; for client detection
                   [com.taoensso/timbre "4.10.0"
                    :exclusions [org.clojure/clojure]] ; needed for onyx-datomic

                   [com.datomic/datomic-pro "0.9.5930"
                    :scope "provided"
                    :exclusions [org.clojure/clojure]]
                   [com.datomic/client-pro "0.8.14"
                    :scope "provided"
                    :exclusions [org.clojure/*]]

                   [adzerk/boot-test "RELEASE" :scope "test" :exclusions [org.clojure/*]]
                   [adzerk/bootlaces "0.1.13"  :scope "test" :exclusions [org.clojure/*]]])


(require '[adzerk.boot-test :refer (test)]
         '[adzerk.bootlaces :refer :all])

(def project 'com.m0smith/datomic-schema)
(def +version+ "0.3.10")

(bootlaces! +version+)


(task-options!
 pom {:project     project
      :version     +version+
      :description "A DSL for Datomic Schema Definitions"
      :url         "https://github.com/m0smtih/datomic-schema"
      :scm         {:url "https://github.com/m0smtih/datomic-schema"}
      :license     {"Eclipse Public License"
                    "http://www.eclipse.org/legal/epl-v10.html"}})

(require '[adzerk.boot-test :refer [test]])
