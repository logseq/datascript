(ns demo.core
  (:require ["fs" :as fs]
            ["path" :as path]
            [datascript.core :as d]
            [datascript.storage :refer [IStorage]]
            [clojure.edn :as edn]))

(defn file-storage
  ([dir]
   (file-storage dir {}))
  ([dir opts]
   (when-not (fs/existsSync dir)
     (fs/mkdirSync dir))
   (reify IStorage
     (-store [_ addr+data-seq]
       (doseq [[addr data] addr+data-seq]
         (let [file-path (path/join dir (str addr))]
           (fs/writeFileSync file-path (pr-str data)))))

     (-restore [_ addr]
       (let [file-path (path/join dir (str addr))]
         (some-> (fs/readFileSync file-path)
                 (.toString)
                 (edn/read-string)))))))
(comment
  ;; restore db from storage
  (def storage (file-storage "/tmp/db"))
  (def conn (d/restore-conn storage))
  ;;
  )

(def tx-data (map
              (fn [i]
                {:db/id (+ 10 i)
                 :data i})
               (range 128)))

(def pconn1 (atom nil))
(defn create-persist-db
  []
  (let [storage (file-storage "/tmp/db")
        conn (d/create-conn nil {:storage storage})]
    (reset! pconn1 conn)
    (d/transact! conn tx-data)))

(def pconn2 (atom nil))
(defn restore-db-test
  []
  (let [storage (file-storage "/tmp/db")
        conn (d/restore-conn storage)]
    (reset! pconn2 conn)
    (prn "Entity 100"
         (:data (d/entity @conn 100)))))

(def mconn (atom nil))
(defn memory-db-test
  []
  (let [conn (d/create-conn)]
    (d/transact! conn tx-data)
    (reset! mconn conn)
    (d/entity @conn 10)))

(defn main
  []
  (create-persist-db)
  (restore-db-test))
