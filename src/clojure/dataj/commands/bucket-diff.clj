(ns dataj.commands.bucket-diff
  (:require [dataj.s3 :as s3]
            [dataj.cli :as cli]
            [clojure.tools.cli :refer [parse-opts]])
  (:gen-class))


(def cli-options
  [["-o" "--origin ORIGIN" "The origin path to match files"]
   ["-i" "--index INDEX" "The index file to check for files"]
   ["-t" "--topic TOPIC" "The sns arn to publish to"]
   ["-h" "--help"]])


(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        origin (options :origin)
        index (options :index)
        topic-arn (options :topic)]
    (cond
      (:help options) (cli/exit 0 summary)
      errors (cli/exit 1 (cli/error-msg errors)))
    (println "Publishing from" origin)
    (->> (s3/get-bucket-diff origin index)
         (map (partial s3/publish-msg topic-arn))
         count
         (println "Published"))))
