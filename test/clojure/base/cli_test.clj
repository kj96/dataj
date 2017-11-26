(ns base.cli-test
  (:require [clojure.test :refer :all]
            [base.cli :refer :all]
            [clojure.string :as s]))

(deftest error-msg-test
  (testing "error-msg returns newline seperated errors"
    (is (= ["Command Parsing Error:" "" "error1" "error2"]
           (s/split (error-msg ["error1" "error2"]) #"\n")))))

(deftest exit-test
  (with-redefs [exit-now! (fn [status] status)]
    (testing "System/exit is called"
      (is (= 1 (exit 1 "test-status"))))))
