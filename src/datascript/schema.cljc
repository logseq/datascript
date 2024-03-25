(ns datascript.schema
  (:require [clojure.string]))

(def schema-keys #{:db/ident :db/isComponent :db/noHistory :db/valueType :db/cardinality :db/unique :db/index :db.install/_attribute :db/doc :db/tupleType :db/tupleTypes :db/tupleAttrs})

(defonce schema-attr?
  #{:db/id :db/ident :db/isComponent :db/valueType :db/cardinality :db/unique :db/index :db/doc :db/tupleAttrs  :db/tupleType :db/tupleTypes})

(defn schema?
  [m]
  (and (:db/ident m)
       (:db/cardinality m)))

(defn is-system-keyword? [value]
  (and (or (keyword? value) (string? value))
       (if-let [ns (namespace (keyword value))]
         (= "db" (first (clojure.string/split ns #"\.")))
         false)))

(defn schema-entity? [entity]
  (some #(contains? entity %) schema-keys))
