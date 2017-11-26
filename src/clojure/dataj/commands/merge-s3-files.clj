(ns dataj.commands.core
  (:require [amazonica.aws.s3 :as s3]
            [base.cli :as base]
            [base.s3 :as s3-utils]
            [clojure.core.async :as async]
            [clojure.tools.cli :refer [parse-opts]])
  (:gen-class))

(def default-parallelism 10)
(def TEST-INPUT "muctesting/magritte-test/muc-events-input/muc-events-log-2014-10-02-0000.gz")
(def TEST-OUTPUT "muctesting/magritte-test/muc-events-output/test.gz")

(def cli-options
  [["-i" "--input INPUT" "The path to grab events from"]
   ["-o" "--output OUTPUT" "The path to write to" :default "local-output"]
   ["-p" "--parallelism PARALLELISM" "The number of processes to spin up for object getting" :default default-parallelism]
   ["-h" "--help"]])

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        input-path (options :input)
        output-path (options :output)
        parallelism (options :parallelism)]
    (cond
      (:help options) (base/exit 0 summary)
      errors (base/exit 1 (base/error-msg errors)))
    (println "Merging objs found at " input-path " to " output-path)
        (s3/put-object (s3-utils/merge-s3-path input-path output-path parallelism))))
