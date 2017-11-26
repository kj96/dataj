(ns dataj.cli
  (:require [clojure.string :as string]))


(defn error-msg [errors]
  (str "Command Parsing Error:\n\n"
       (string/join \newline errors)))

(defn exit-now!
  "Indirection for System/exit. Needed to allow base.cli/exit to be testable
   http://stackoverflow.com/questions/29289151/is-there-a-way-to-test-system-exit-in-clojure"
  [status]
  (System/exit status))

(defn exit [status msg]
  (println msg)
  (exit-now! status))
