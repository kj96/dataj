(ns base.spark-test
  (:require [clojure.test :refer :all]
            [flambo.api :as f]
            [base.spark :refer :all]))

(deftest new-spark-context-aws-default-test
  (let [sc (new-spark-context-aws "test")]
   (testing "that sc is a delay"
     (is (= clojure.lang.Delay (type sc))))
   (testing "that one can start a spark context"
     (is (= org.apache.spark.api.java.JavaSparkContext
            (type @sc))))
   (testing "that default spark context has master=local"
     (is (= "local" (.master @sc))))
   (testing "that spark context has specified app name"
     (is (= "test" (.appName @sc))))
   (testing "that default spark context has no aws creds"
     (let [hadoopContext (.hadoopConfiguration @sc)]
       (is (nil? (.get hadoopContext "fs.s3n.awsSecretAccessKey")))
       (is (nil? (.get hadoopContext "fs.s3n.awsAccessKeyId")))))
  (.stop @sc)))


(deftest new-spark-context-aws-with-creds-test
  (let [sc (new-spark-context-aws
            "test" :aws-access-key "access-key"
            :aws-secret-key "secret-key")]
   (testing "that default spark context has no aws creds"
     (let [hadoopContext (.hadoopConfiguration @sc)]
       (is (= "secret-key" (.get hadoopContext "fs.s3n.awsSecretAccessKey")))
       (is (= "access-key" (.get hadoopContext "fs.s3n.awsAccessKeyId")))))
  (.stop @sc)))

(deftest config-spark-test
  (let [s-conf (config-spark "test")]
   (testing "config-spark returns a spark config"
     (is (= org.apache.spark.SparkConf (type s-conf))))
   (testing "config-spark uses app-name arg"
     (is (= "test" (.get s-conf "spark.app.name"))))))

(deftest config-hadoop-test
  (let [sc (f/spark-context (config-spark "test"))]
    (let [hadoopConf (.hadoopConfiguration (config-hadoop sc))]
      (testing "config-hadoop doesn't set aws creds when not provided"
        (is (nil? (.get hadoopConf "fs.s3n.awsAccessKeyId")))
        (is (nil? (.get hadoopConf "fs.s3n.awsSecretAccessKey"))))
      (.stop sc))
    (let [hadoopConf (.hadoopConfiguration (config-hadoop sc "access" "secret"))]
      (testing "config-hadoop sets aws cred when provided"
        (is (= "access" (.get hadoopConf "fs.s3n.awsAccessKeyId")))
        (is (= "secret" (.get hadoopConf "fs.s3n.awsSecretAccessKey"))))
      (.stop sc))))
