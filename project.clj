(defproject puhrez/dataj "0.1.0"
  :description "A base library for working with data pipeline services in clojure"
  :url "http://github.com/puhrez/dataj"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [environ "0.5.0"]
                 [amazonica "0.3.52"]
                 [byte-streams "0.2.1"]
                 [byte-transforms "0.1.3"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/core.async "0.2.374"]
                 [yieldbot/flambo "0.7.1"]]
  :target-path "target/%s"
  :scm {:dir ".."}
  :source-paths ["src/clojure"]
  :test-paths ["test/clojure"]
  :profiles {:provided
             {:dependencies
              [[org.apache.spark/spark-core_2.10 "1.5.0"]]}
             :uberjar {:aot :all}
             :dev
             {:aot [flambo.function]}})
