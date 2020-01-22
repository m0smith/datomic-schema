(ns datomic.schema
  (:refer-clojure :exclude [partition namespace fn])
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [onyx.datomic.api :as api-helper]))

(defrecord Schema [partition ns tx-data
                   key-mappings coercions spec])

(defn schema? [ent]
  (or (instance? datomic.schema.Schema ent)
      (::schema? (meta ent))))

(defn enum? [ent]
  (::enum? (meta ent)))

(defn partition? [ent]
  (or (:db.install/_partition ent)
      (::partition? (meta ent))))

(when (try
        (require '[clojure.spec.alpha :as s])
        true
        (catch Exception e))
  (load "spec-impl"))

(defn create-schema [m]
  (with-meta (map->Schema
              (assoc m :spec (atom nil)))
    (meta m)))


;; (defn on-cloud? []
;;   "Running on the cloud?  Better use the Client API"
;;   (let [env (System/getenv)
;;         rtnval (get env "DEPLOYMENT_ID")]
;;     rtnval))


;; (defn has-peer-lib? [])
;;   (try
;;     (require 'datomic.api)
;;     true
;;     (catch Throwable e
;;       false))

(def peer? (api-helper/peer?))

;; (def peer?
;;   (or (not (on-cloud?))
;;       (has-peer-lib?))
;; )

(if peer?
  (def tempid @(resolve 'datomic.api/tempid))
  (defn tempid
    ([part]    (.toString (java.util.UUID/randomUUID)))
    ([part id] (str id))))

(defn qualify-keyword 
  ([ns k] (qualify-keyword ns k false))
  ([ns k allow-nil?]
   (let [k (keyword k)]
     (when (and (not allow-nil?) (nil? k))
       (throw (ex-info "keyword cannot be nil" {:ns ns :allow-nil? allow-nil?})))
     (if (or (nil? ns) (nil? k) (clojure.core/namespace k))
       k
       (keyword (name ns) (name k))))))

(defn map-keys [f m]
  (reduce-kv #(assoc %1 (f %2) %3) {} m))

(defn partition [ent part]
  (assert peer? "partition form must working with peer lib")
  (let [part (if (map? part)
               part
               {:ident part})
        part (map-keys #(qualify-keyword "db" %) part)
        part (assoc part :db.install/_partition :db.part/db)]
    (-> ent
        (assoc  :partition (:db/ident part))
        (update :tx-data assoc (:db/ident part) part))))

(defn namespace [ent ns]
  (assoc ent :ns ns))

(defn attrs
  ([ent a & as]
   (reduce attrs ent (cons a as)))
  ([ent a]
   (let [a    (if (map? a)
                a
                (let [[ident type opts] a]
                  (merge opts
                         {:ident     ident
                          :valueType type})))
         a    (map-keys #(qualify-keyword "db" %) a)
         type (:db/valueType a)
         a    (cond-> a
                true
                (update :db/ident #(qualify-keyword (:ns ent) %))

                true
                (update :db/valueType
                        #(if (var? %)
                           :db.type/ref
                           (qualify-keyword "db.type" %)))

                true
                (update :db/cardinality
                        #(if %
                           (qualify-keyword "db.cardinality" %)
                           :db.cardinality/one))

                (:db/unique a)
                (update :db/unique #(qualify-keyword "db.unique" %)))

         spec (if (var? type)
                type
                (:db/valueType a))]
     (-> ent
         (update :tx-data assoc (:db/ident a) a)
         (assoc-in [:coercions (:db/ident a)] spec)))))

(defn enums
  ([ent x & xs]
   (reduce enums ent (cons x xs)))
  ([ent x]
   (let [x       (if (map? x)
                   x
                   {:db/ident x})
         ns      (:ns ent)
         x       (update x :db/ident #(qualify-keyword ns %))
         x       (map-keys #(qualify-keyword ns %) x)
         x       (assoc x :db/id
                        (or (:db/id x)
                            (tempid (:partition ent))))
         depends #{(:partition ent)}]
     (-> ent
         (update :tx-data assoc (:db/ident x)
                 (vary-meta x assoc
                            ::enum?   true
                            ::depends depends))
         (vary-meta assoc ::enum? true)))))

(defn fn'
  ([ent fname bindings body]
   (if-not (schema? ent)
     (fn' ent fname (cons bindings body))
     (let [fname (qualify-keyword (str "fn." (:ns ent)) fname)]
       (update ent :tx-data assoc fname
               (fn' fname bindings body)))))
  ([name bindings body]
   (let [body (if (= 1 (count body))
                (first body)
                (cons 'do body))]
     {:db/ident name
      :db/fn    ((resolve 'datomic.api/function)
                 (merge (meta bindings)
                        {:lang   "clojure"
                         :params bindings
                         :code   body}))})))

(defmacro fn [name bindings & body]
  (assert peer? "fn form must working with peer lib")
  `(fn' ~name '~bindings '~(first body) '~(rest body)))

(defn raws [ent & xs]
  (update ent :tx-data merge
          (zipmap (repeatedly gensym) xs)))

(defn depends [schema]
  (->> (dissoc schema :db/ident)
       (apply concat)
       (set)
       (set/union (::depends (meta schema)))))

(defn schemas
  ([] (apply schemas (all-ns)))
  ([& nses]
   (->> nses
        (mapcat (comp vals ns-publics))
        (filter schema?)
        (map var-get))))

(defn tx-datas
  ([] (apply tx-datas (schemas)))
  ([& ents]
   (let [tx-data (->> (map :tx-data ents)
                      (apply merge-with merge))]
     (loop [datas  ()
            idents (set (keys tx-data))]
       (if (empty? idents)
         datas
         (let [deps  (->> (map tx-data idents)
                          (map depends)
                          (apply set/union))
               data  (->> (set/difference idents deps)
                          (map tx-data))
               inter (set/intersection deps idents)]
           (recur (cons data datas) inter)))))))

(defn camel->ns [clazz]
  (-> (name clazz)
      (str/replace-first #"\w" str/lower-case)
      (str/replace #"[A-Z]" #(str "." (str/lower-case %)))
      (symbol)))

(defn wrap-key-mappings [{:as ent tx-data :tx-data}]
  (let [idents (keys tx-data)]
    (assoc ent :key-mappings
           (zipmap (map (comp keyword name) idents) idents))))


(declare coerce)

(defn satisfy-schema 
  ([s m] (satisfy-schema s m false))
  ([{:as ent :keys [partition key-mappings coercions]} m allow-nil?]
   (if (enum? ent)
     (qualify-keyword (:ns ent) m allow-nil?)
     (reduce-kv (clojure.core/fn [e k v]
                  (let [k (key-mappings k k)
                        c (coercions k)
                        new-v (coerce c v allow-nil?)]
                    (if (or allow-nil? (not (nil? new-v)))
                      (assoc e k new-v)
                      e)))
                {:db/id (or (:db/id m) (tempid partition))}
                m))))

(defn coerce 
  ([c m] (coerce c m false))
  ([c m allow-nil?]
   (cond
     (var? c)
     (coerce (var-get c) m allow-nil?)

     (schema? c)
     (if (sequential? m)
       (map #(satisfy-schema c % allow-nil?) m)
       (satisfy-schema c m allow-nil?))
     :else m)))

(declare apply-key-mappings)

(defn apply-key-mapping 
  "Recusively assoc key `k` with value `v` in `acc` using the schema to
  transaction key name."
  [{:keys [key-mappings coercions]} acc k v]
  (let [new-k (key-mappings k k)
        coercion (coercions new-k)
        new-v (if (and v (var? coercion))
            (apply-key-mappings (var-get coercion) v)
            v)]
    (assoc acc new-k new-v)))

(defn apply-key-mappings 
  "Recusively rename keys in `m` to the schema keys."
  [s m]
  (cond
    (nil? m) m
    (and (enum? s) (keyword? m)) (coerce s m)
    (map? m) (reduce-kv (partial apply-key-mapping s) {} m)
    (vector? m) (mapv #(reduce-kv (partial apply-key-mapping s) {} %) m)
    (seq m) (map #(reduce-kv (partial apply-key-mapping s) {} %) m)
    :else m))


(defmacro defschema [name & decls]
  `(do
     (def ~(with-meta name {::schema? true})
       (-> {:partition :db.part/user
            :ns        '~(camel->ns name)
            :tx-data   {}
            :coercions {}}
           (with-meta {::schema? true})
           ~@decls
           (wrap-key-mappings)
           (vary-meta assoc ::schema? true)
           (create-schema)))
     (defn ~(symbol (str "->" name)) 
       "allow-nil? is useful for doing upserts"
       ([m#] (~(symbol (str "->" name)) m# false))
       ([m# allow-nil?#]
        (let [rtnval#  (coerce ~name m# allow-nil?#)]
          rtnval#)))))

(defmacro defentity
  {:deprecated "0.1.7"}
  [name & decls]
  `(defschema ~name ~@decls))

;; (defn client-conn? [conn] 
;;   "Check for the client api"
;;   (let [ifs (str (supers (class conn)))]
;;     (re-find #"datomic.client.(api|impl)" ifs)))

;; (defn- client-conn?-old [conn]
;;   (or  (= (type conn) (resolve 'datomic.client.impl.shared.Connection))
;;        (= (type conn) (resolve 'datomic.client.impl.local.Connection))))

;; (defn- peer-conn? [conn]
;;   (not (client-conn? conn)))

(defmacro with-alias [aliases & body]
  (let [names    (map str aliases)
        ns-names (set names)
        aliases  (apply array-map names)
        w        (clojure.core/fn w [body]
                   (walk/walk
                    (clojure.core/fn [x]
                      (cond
                        (and (symbol? x)
                             (ns-names (clojure.core/namespace x)))
                          (let [ns  (clojure.core/namespace x)
                                ns  (aliases ns ns)
                                sym (symbol ns (name x))]
                            `(if (resolve '~sym) (var-get (resolve '~sym))))

                        (coll? x)
                          (w x)

                        :else
                          x))
                    identity
                    body))]
    (cons
     'do
     (w body))))

(defn install
  ([conn] (apply install conn (schemas)))
  ([conn & schemas-or-nses]
   (with-alias [d datomic.api
                c datomic.client.api]
     (let [scms  (mapcat #(if (schema? %)
                            [%]
                            (schemas (the-ns %)))
                         schemas-or-nses)
           ; _ (log/debug "INSTALL scms" scms)
           ; peer? (peer-conn? conn)
           trans (if peer?
                   #(deref (d/transact conn %))
                   #(c/transact conn {:tx-data %}))

           updated? (clojure.core/fn [tx-data]
                      (let [report (if peer?
                                     (d/with (d/db conn) tx-data)
                                     (c/with (c/with-db conn) {:tx-data tx-data}))]
                        (> (count (:tx-data report)) 1)))]
       (doseq [tx    (apply tx-datas scms)
               :when (updated? tx)]
         (trans tx))))))
