(ns cosmetics.core
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [clojure.string :as str]))

(def couch-url "http://127.0.0.1:5984")
(def db-name   "cosmetics")
(def auth      {:basic-auth ["admin" "mtu12345"]})

(defn ensure-db! []
  ;; Create DB 
  (let [resp (http/put (str couch-url "/" db-name)
                       (merge auth {:throw-exceptions false}))]
    (when-not (#{201 202 412} (:status resp))
      (throw (ex-info "Failed creating Database"
                      {:status (:status resp) :body (:body resp)})))))

(defn read-csv->maps
  "Return seq of maps for a CSV file with headers"
  [^String path]
  (with-open [r (io/reader path)]
    (let [rows (doall (csv/read-csv r))
          headers (mapv #(-> % str/trim
                             (str/replace #" " "_")
                             (str/lower-case))
                        (first rows))
          data (rest rows)]
      (map (fn [row] (zipmap headers row)) data))))

(defn bulk-insert!
  "Insert documents in batches using _bulk_docs."
  [docs]
  (let [endpoint (str couch-url "/" db-name "/_bulk_docs")]
    (http/post endpoint
               (merge auth
                      {:headers {"Content-Type" "application/json"}
                       :body    (json/encode {:docs (vec docs)})}))))

(defn import-csv!
  "Import csv file, streams CSV rows in batches."
  [csv-path]
  (println "Ensuring DB exists:" db-name)
  (ensure-db!)
  (println "Reading CSV:" csv-path)
  (let [rows (read-csv->maps csv-path)
        batch-size 500]
    (doseq [batch (partition-all batch-size rows)]
      (let [resp (bulk-insert! batch)]
        (when-not (<= 200 (:status resp) 299)
          (throw (ex-info "Bulk insert failed" {:status (:status resp) :body (:body resp)}))))))
  (println "Import complete. Open Fauxton:")
  (println "http://127.0.0.1:5984/_utils/#/database/cosmetics/_all_docs"))

;;CRUD examples
(defn create-doc!
  "Create a document (let CouchDB assign _id)."
  [m]
  (http/post (str couch-url "/" db-name)
             (merge auth {:headers {"Content-Type" "application/json"}
                          :body (json/encode m)})))

(defn read-doc
  "Read by id."
  [id]
  (http/get (str couch-url "/" db-name "/" id) auth))

(defn update-doc!
  "Replace doc: requires latest _rev."
  [id rev m]
  (http/put (str couch-url "/" db-name "/" id)
            (merge auth {:headers {"Content-Type" "application/json"}
                         :body (json/encode (assoc m :_id id :_rev rev))})))

(defn delete-doc!
  "Delete by id + rev."
  [id rev]
  (http/delete (str couch-url "/" db-name "/" id)
               (merge auth {:query-params {"rev" rev}})))

;; Entry point for CLI
(defn -main
  [& args]
  (if-let [csv-path (first args)]
    (import-csv! csv-path)
    (println "Usage: clj -M -m cosmetics.core \"C:\\Users\\oksan\\Desktop\\most_used_beauty_cosmetics_products_extended.csv\"")))
