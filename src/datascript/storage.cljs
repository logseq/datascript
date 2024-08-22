(ns datascript.storage
  (:require
   [datascript.db :as db :refer [Datom]]
   [datascript.util :as util]
   [me.tonsky.persistent-sorted-set.protocol :as set-protocol]
   [me.tonsky.persistent-sorted-set :as set :refer [BTSet Node Leaf]]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]))

(defprotocol IStorage
  ;; :extend-via-metadata true

  (-store [_ addr+data-seq delete-addrs]
    "Gives you a sequence of `[addr data]` pairs to serialize and store.

     `addr`s are 64 bit integers.
     `data`s are clojure-serializable data structure (maps, keywords, lists, integers etc)")

  (-restore [_ addr]
    "Read back and deserialize data stored under single `addr`"))

(def ^:private ^:dynamic *store-buffer*)
;; unused addresses
(def ^:private *delete-buffer (atom {}))

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
  (store [_ ^Node node address]
    (let [addr (cond
                 (and address (instance? Node node)
                      (contains? (set (.-_addresses node)) address))
                 (gen-addr)
                 :else
                 (or address (gen-addr)))
          keys (mapv serializable-datom (.-keys node))
          data (cond-> {:keys keys}
                 (instance? Node node)
                 (assoc :addresses (.-_addresses node)))]
      (vswap! *store-buffer* conj! [addr data])
      addr))
  (restore [_ addr]
    (when addr
      (let [{:keys [keys addresses]} (-restore storage addr)]
        (when keys
          (let [keys' (->> (map (fn [[e a v tx]] (db/datom e a v tx)) keys)
                           (arrays/into-array))]
            (if addresses
              (let [children (arrays/make-array (count addresses))]
                (set/new-node keys' children addresses addr false))
              (set/new-leaf keys' addr false)))))))
  (accessed [_ _addr]
    ;; TODO:
    nil)

  (delete [_ unused-addresses]
    (swap! *delete-buffer update storage
           (fn [buffer]
             (into buffer (remove nil? unused-addresses))))))

(defn make-storage-adapter [storage _opts]
  (StorageAdapter. storage))

(defn storage-adapter ^StorageAdapter [db]
  (when db
    (.-storage ^BTSet (:eavt db))))

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
          (let [storage (:storage adapter)
                delete-addrs (->> (get @*delete-buffer storage)
                                  (distinct))
                _ (swap! *delete-buffer assoc storage nil)]
            (-store storage (persistent! @*store-buffer*) delete-addrs)))
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
     (let [settings (.-_settings ^BTSet (:eavt db))
           adapter  (StorageAdapter. storage)]
       (store-impl! db adapter false)))))

(defn store-tail [db tail]
  (-store (storage db) [[tail-addr (mapv #(mapv serializable-datom %) tail)]] nil))

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
     (if (empty? datoms)
       db
       (try
         (as-> db %
           (reduce db/with-datom % datoms)
           (assoc % :max-tx (:tx (first datoms))))
         (catch :default e
           (js/console.error e)
           db))))
   db tail))

(defn restore
  ([storage]
   (restore storage {}))
  ([storage opts]
   (let [[db tail] (restore-impl storage opts)]
     (db-with-tail db tail))))

(defn maybe-adapt-storage [opts]
  (if-some [storage (:storage opts)]
    (update opts :storage make-storage-adapter opts)
    opts))
