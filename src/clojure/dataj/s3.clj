(ns dataj.s3
  (:require [amazonica.aws.s3 :as aws-s3]
            [clojure.string :as s]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
            [byte-streams :as bs]
            [byte-transforms :as bt]
            [clojure.set :as set]
            [amazonica.aws.sns :as sns]
            [clojure.tools.logging :as log])
  (:import [java.io SequenceInputStream]))

(def empty-input-stream (bs/to-input-stream (byte-array [])))
(def default-parallelism 10)

(defmacro catch-with-logs
  "hacky wrapper for dev"
  [form]
  `(try
     ~form
     (catch ~Exception ex# (log/error ex#))))

(defn list-next-objects
  [listing]
  (aws-s3/list-objects
   :bucket-name (:bucket-name listing)
   :prefix (:prefix listing)
   :marker (:next-marker listing)))

(defn get-s3-obj-summaries
  [bucket prefix]
  (->> (loop [listings [(aws-s3/list-objects bucket prefix)]]
         (let [current-listing (last listings)]
           (if (:truncated? current-listing)
             (recur (conj listings (list-next-objects current-listing)))
             listings)))
       (mapcat :object-summaries)
       (filter #(not= (last (:key %)) \/)))) ;;remove folders

(defn s3-path->bucket+prefix
  [s3-path]
  (let [split-path (s/split s3-path #"/")
        bucket (first split-path)
        prefix (s/join "/" (rest split-path))]
    [bucket prefix]))

(defn s3-path->s3-obj-summaries
  [s3-path]
  (let [[b k] (s3-path->bucket+prefix s3-path)]
    (get-s3-obj-summaries b k)))

(defn get-object
  [bucket k]
  (catch-with-logs (aws-s3/get-object bucket k)))

(def s3-obj-summaries->s3-objs
  (comp (map (juxt :bucket-name :key))
        (map (partial apply get-object))))

(defn s3-path->s3-objs
  "given an s3-path returns a set of the children paths"
  [s3-path]
  (->> (s3-path->s3-obj-summaries s3-path)
       (sequence s3-obj-summaries->s3-objs)))

(def s3-obj->size+stream (juxt #(get-in % [:object-metadata :content-length]) :object-content))
(def s3-obj-summaries->size+streams
  (comp s3-obj-summaries->s3-objs
        (map s3-obj->size+stream)))

(defn combine-sizes+streams
  "stream reducer"
  [x y]
  (let [x-count (first x)
        y-count (first y)
        x-stream (second x)
        y-stream (second y)]
    [(+ x-count y-count)
     (SequenceInputStream. x-stream, y-stream)]))

(defn merge-s3-path
  "Take the s3 objs found at `input` and merge their content into `output`"
  [input output & [parallelism]]
  (let [parallelism (or parallelism default-parallelism)
        input-objs-summaries (s3-path->s3-obj-summaries input)
        input-objs-summaries-chan (async/to-chan input-objs-summaries)
        size+stream-chan (async/chan)
        [bucket k] (s3-path->bucket+prefix output)
        output-size+stream (->> (get-object bucket k)
                               s3-obj->size+stream)
        put-object-req {:bucket-name bucket :key k}
        initial-size+stream (if (first output-size+stream)
                              output-size+stream
                              [0 empty-input-stream])]
    (async/pipeline-blocking parallelism size+stream-chan
                             s3-obj-summaries->size+streams
                             input-objs-summaries-chan)
    (let [size+stream (-> (async/reduce combine-sizes+streams
                                            initial-size+stream
                                            size+stream-chan)
                          async/<!!)
          content-size (first size+stream)
          stream (second size+stream)]
      (-> (assoc put-object-req :input-stream stream)
          (assoc-in [:object-metadata :content-length] content-size)))))

(defn get-bucket-diff
  [origin index]
  (let [origin-files (->> origin
                          s3-path->s3-obj-summaries
                          (map #(s/join "/" ((juxt :bucket-name :key) %)))
                          set)
        index-file (->> (s3-path->bucket+prefix
                         index)
                        (apply get-object))
        index-files (when index-file
                      (-> index-file
                          :object-content
                          bs/to-line-seq
                          set))]
    (if index-files
      (set/difference origin-files index-files)
      origin-files)))


;; SNS ;)

(defn publish-msg
  [topic-arn msg]
  (println "publishing" msg "to" topic-arn)
  (sns/publish :topic-arn topic-arn
               :message msg))

(defn events->sns
  [topic-arn]
  (comp
   (map (partial publish-msg topic-arn))))
