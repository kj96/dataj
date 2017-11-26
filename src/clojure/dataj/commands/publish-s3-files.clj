(ns dataj.commands.publish-s3-files
  (:gen-class)
  (:require [dataj.cli :as cli]
            [dataj.s3 :as s3]
            [amazonica.aws.sns :as sns]
            [clojure.core.async :as async]
            [clojure.string :as s]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]))

(def default-parallelism 50)

(def cli-options
  [["-i" "--input INPUT" "The s3 path to grab events from"]
   ["-o" "--output OUTPUT" "The sns topic arn to publish to"]
   ["-p" "--parallelism PARALLELISM" "The number of processes to use"
    :default default-parallelism
    :parse-fn #(Integer/parseInt %)]
   ["-h" "--help"]])

(def s3-obj-summaries->s3-spark-objs
  (comp (filter #(s/includes? (:key %) "part")) ;; get spark output only
        s3-utils/s3-obj-summaries->s3-objs))


(defn publish-events
  [object-summaries topic-arn parallelism]
  (let [summaries-chan (async/to-chan object-summaries)
        objects-chan (async/chan)
        lines-chan (async/chan)
        publish-chan (async/chan)
        handle-ex #(log/error %)]
    (async/pipeline-blocking parallelism objects-chan s3-obj-summaries->s3-spark-objs summaries-chan handle-ex)
    (async/pipeline-blocking parallelism lines-chan s3/s3-objs->json-events objects-chan handle-ex)
    (async/pipeline-blocking parallelism publish-chan (s3/events->sns topic-arn) lines-chan handle-ex)
    (async/reduce (fn [n _] (inc n)) 0 publish-chan)))

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        s3-path (options :input)
        topic-arn (options :output)
        parallelism (options :parallelism)]
    (cond
      (:help options) (cli/exit 0 summary)
      errors (cli/exit 1 (cli/error-msg errors)))
    (log/info (str "Publishing files at s3://" s3-path " to SNS-topic " topic-arn))
    (-> (s3-utils/s3-path->s3-obj-summaries s3-path)
        (publish-events topic-arn parallelism)
        async/<!!
        (log/info "events published"))
        (log/info "DONE!")))
