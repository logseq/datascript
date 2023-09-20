(ns datascript.storage
  (:require
   [datascript.db :as db :refer [Datom]]
   [datascript.util :as util]
   [me.tonsky.persistent-sorted-set.impl :refer [PersistentSortedSet Node Leaf]]
   [me.tonsky.persistent-sorted-set.protocol :as set-protocol]
   [me.tonsky.persistent-sorted-set :as set]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]))

(defprotocol IStorage
  ;; :extend-via-metadata true

  (-store [_ addr+data-seq]
    "Gives you a sequence of `[addr data]` pairs to serialize and store.

     `addr`s are 64 bit integers.
     `data`s are clojure-serializable data structure (maps, keywords, lists, integers etc)")

  (-restore [_ addr]
    "Read back and deserialize data stored under single `addr`"))

(def ^:private ^:dynamic *store-buffer*)

(defn serializable-datom [^Datom d]
  [(.-e d) (.-a d) (.-v d) (.-tx d)])

(def ^:private root-addr
  0)

(def ^:private tail-addr
  1)

(defonce ^:private *max-addr
  (volatile! 1000000))

(defn- gen-addr []
  (vswap! *max-addr inc))

(defrecord StorageAdapter [storage]
  set-protocol/IStorage
  (store [_ ^Node node]
    (let [addr (gen-addr)
          keys (mapv serializable-datom (.-keys node))
          data (cond-> {:keys keys}
                 (instance? Node node)
                 (assoc :addresses (.-addresses node)))]
      (vswap! *store-buffer* conj! [addr data])
      addr))
  (restore [_ addr]
    (let [{:keys [keys addresses]} (-restore storage addr)
          keys' (->> (map (fn [[e a v tx]] (db/datom e a v tx)) keys)
                     (arrays/into-array))]
      (if addresses
        (Node. keys' (arrays/make-array (count addresses)) addresses)
        (Leaf. keys'))))
  (accessed [_ address]
    ;; TODO:
    nil))

(defn make-storage-adapter [storage _opts]
  (StorageAdapter. storage))

(defn storage-adapter ^StorageAdapter [db]
  (when db
    (.-storage ^PersistentSortedSet (:eavt db))))

(defn storage [db]
  (when-some [adapter (storage-adapter db)]
    (:storage adapter)))

(def ^:private ^List stored-dbs
  (atom []))

(defn- remember-db [db]
  (swap! stored-dbs conj db))

(defn store-impl! [db adapter force?]
  (locking (:storage adapter)
    (remember-db db)
    (binding [*store-buffer* (volatile! (transient []))]
      (let [eavt-set (:eavt db)
            aevt-set (:aevt db)
            avet-set (:avet db)
            eavt-addr (set/store eavt-set adapter)
            aevt-addr (set/store aevt-set adapter)
            avet-addr (set/store avet-set adapter)
            meta (merge
                  {:schema   (:schema db)
                   :max-eid  (:max-eid db)
                   :max-tx   (:max-tx db)
                   :eavt     eavt-addr
                   :aevt     aevt-addr
                   :avet     avet-addr
                   :eavt-metadata {:count (.-cnt eavt-set)
                                   :shift (.-shift eavt-set)}
                   :aevt-metadata {:count (.-cnt aevt-set)
                                   :shift (.-shift aevt-set)}
                   :avet-metadata {:count (.-cnt avet-set)
                                   :shift (.-shift avet-set)}
                   :max-addr @*max-addr}
                  (set/settings (:eavt db)))]
        (when (or force? (pos? (count @*store-buffer*)))
          (vswap! *store-buffer* conj! [root-addr meta])
          (vswap! *store-buffer* conj! [tail-addr []])
          (-store (:storage adapter) (persistent! @*store-buffer*)))
        db))))

(defn store
  ([db]
   (if-some [adapter (storage-adapter db)]
     (store-impl! db adapter false)
     (throw (ex-info "Database has no associated storage" {}))))
  ([db storage]
   (if-some [adapter (storage-adapter db)]
     (let [current-storage (:storage adapter)]
       (if (identical? current-storage storage)
         (store-impl! db adapter false)
         (throw (ex-info "Database is already stored with another IStorage" {:storage current-storage}))))
     (let [settings (.-_settings ^PersistentSortedSet (:eavt db))
           adapter  (StorageAdapter. storage)]
       (store-impl! db adapter false)))))

(defn store-tail [db tail]
  (-store (storage db) [[tail-addr (mapv #(mapv serializable-datom %) tail)]]))

(defn restore-impl [storage opts]
  (locking storage
    (when-some [root (-restore storage root-addr)]
      (let [tail    (-restore storage tail-addr)
            {:keys [schema eavt aevt avet max-eid max-tx max-addr
                    eavt-metadata aevt-metadata avet-metadata]} root
            _       (vswap! *max-addr max max-addr)
            opts    (merge root opts)
            adapter (make-storage-adapter storage opts)
            db      (db/restore-db
                     {:schema  schema
                      :eavt    (set/restore-by db/cmp-datoms-eavt eavt adapter (assoc opts :set-metadata eavt-metadata))
                      :aevt    (set/restore-by db/cmp-datoms-aevt aevt adapter (assoc opts :set-metadata aevt-metadata))
                      :avet    (set/restore-by db/cmp-datoms-avet avet adapter (assoc opts :set-metadata avet-metadata))
                      :max-eid max-eid
                      :max-tx  max-tx})]
        (remember-db db)
        [db (mapv #(mapv (fn [[e a v tx]] (db/datom e a v tx)) %) tail)]))))

(defn db-with-tail [db tail]
  (reduce
    (fn [db datoms]
      (reduce db/with-datom db datoms))
    db tail))

(defn restore
  ([storage]
   (restore storage {}))
  ([storage opts]
   (let [[db tail] (restore-impl storage opts)]
     (db-with-tail db tail))))
