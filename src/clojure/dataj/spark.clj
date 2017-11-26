(ns dataj.spark
  (:refer-clojure :exclude [group-by])
  (:require [environ.core :refer [env]]
            [flambo.conf :as conf]
            [clojure.string :as s]
            [flambo.sql :as fsql]
            [cheshire.core :refer [parse-string]]
            [flambo.api :as f])
  (:import [org.apache.spark.sql RowFactory Column GroupedData functions]
             [org.apache.spark.sql.types
              StructType StructField
              Metadata DataTypes])))

(defn config-spark
  "configures spark with the app-name and with an optional master, defaulting to local"
  [app-name & [spark-master-ip spark-master-port]]
  (-> (conf/spark-conf)
      (conf/master (if (and spark-master-ip spark-master-port)
                     (str "spark://" spark-master-ip ":" spark-master-port)
                     "local"))
      (conf/app-name app-name)))

(defn config-hadoop
  "configures hadoop to use the optional aws credentials"
  [sc & [aws-access-key aws-secret-key]]
  (let [hadoopConf (.hadoopConfiguration sc)]
    (when (and aws-access-key aws-secret-key)
      (.set hadoopConf "fs.s3n.awsAccessKeyId" aws-access-key)
      (.set hadoopConf "fs.s3n.awsSecretAccessKey" aws-secret-key))
    sc))

(defn new-spark-context-aws
  "Creates a delayed spark context with AWS access provided by env vars"
  [app-name & {:keys
               [aws-access-key aws-secret-key spark-master-ip spark-master-port]
               :or
               {aws-access-key (env :aws-access-key)
                aws-secret-key (env :aws-secret-key)
                spark-master-ip (env :spark-master-ip)
                spark-master-port (env :spark-master-port)}}]
  (delay (-> (config-spark app-name spark-master-ip spark-master-port)
              f/spark-context
              (config-hadoop aws-access-key aws-secret-key))))

(defn json->schema-tsv
    "given a schema, returns a function that applys that schema to a json event"
  [json schema]
  (let [event (parse-string json true)]
    (s/join \tab
            (map (partial get-in event) schema))))

(defn parse-schema
  "given path to schema file where desired paths are `.` delimited,
  return a vector of `keywords`"
  [path-to-schema]
  (-> (f/text-file @sc path-to-schema)
      (f/map (f/fn [p]
               (map keyword
                    (s/split p #"\."))))
      f/collect))

(defn path-json->schema-tsv
  "parses the json at `input-path` with the schema at `schema-path`"
  [input-path schema-path]
  (let [schema (parse-schema schema-path)]
    (-> (f/text-file @sc input-path)
        (f/map (f/fn [json] (json->schema-tsv json schema))))))


;; SparkSQL Helpers

(defn schema->columns
  "Takes a `.` delimited schema file path and returns it so to be read into
  a SparkSQL Dataframe"
  [path-to-schema]
  (-> (f/text-file @sc path-to-schema)
      (f/map
       (f/fn [p] (s/replace p #"\." "_")))
      f/collect))

(defn create-struct-type
  [cols]
  (StructType.
   (into-array StructField
               (map #(StructField. % (DataTypes/StringType) true (Metadata/empty)) ;; TODO better way to declare types
                    cols))))

(defn parse-and-read-tsv
  [tsv-path]
  (-> (f/text-file @sc tsv-path)
      (f/map (f/fn [x] (s/split x #"\t")))))

(f/defsparkfn vec->row
  [vs]
  (RowFactory/create (into-array Object vs)))

(defn rdd->df
  [rdd cols]
  (let [rows-rdd (f/map rdd vec->row)
        schema (create-struct-type cols)]
    (.createDataFrame sql rows-rdd schema)))

(defn cols->df-cols
  [df & cols]
  (into-array (map #(.col df %) cols)))

(defn select
  [df & cols]
  (let [df-cols (apply (partial cols->df-cols df) cols)]
    (.select df df-cols)))

(defn group-by
  [df & cols]
  (let [df-cols (apply (partial cols->df-cols df) cols)]
    (.groupBy df df-cols)))

(defn agg
  [^GroupedData gd col->func]
    (.agg gd (java.util.HashMap. col->func)))
