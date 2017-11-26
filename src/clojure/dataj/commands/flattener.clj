(ns dataj.commands.flattener
  (:require [flambo.api :as f]
            [dataj.spark :as [spark]]
            [dataj.cli :as base]
            [clojure.tools.cli :refer [parse-opts]])
  (:gen-class))

(defonce sc (spark/new-spark-context-aws "dataj-flattener"))

(def cli-options
  [["-i" "--input INPUT" "The path to read from"]
   ["-o" "--output OUTPUT" "The path to write to" :default "local-output"]
   ["-s" "--schema SCHEMA" "The path to the schema to use"]
   ["-h" "--help"]]) ;TODO add/handle format option


(defn -main
    "given an input path, schema-path, and an output path writes the relevant events according to a schema as tsv"
  [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        input-path (options :input)
        output-path (options :output)
        schema-path  (options :schema)]
    (cond
      (:help options) (base/exit 0 summary)
      errors (base/exit 1 (base/error-msg errors)))
    (-> (spark/path-json->schema-tsv input-path schema-path)
                (f/save-as-text-file output-path))))
