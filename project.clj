(defproject mysql-queue "1.0.0"
  :description "A durable queue with scheduled job support that is backed by MySQL."
  :url "https://github.com/wildbit/mysql-queue"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/core.async "0.3.465"]
                 [org.clojure/java.jdbc "0.7.5"]
                 [mysql/mysql-connector-java "5.1.28"]]
  :scm {:name "git" :url "https://github.com/wildbit/mysql-queue"}
  :profiles {:1.7 {:dependencies [[org.clojure/clojure "1.7.0"]]}
             :1.8 {:dependencies [[org.clojure/clojure "1.8.0"]]}
             :1.9 {:dependencies [[org.clojure/clojure "1.9.0"]]}}
  :aliases {"test-all" ["with-profile" "+1.7:+1.8:+1.9" "test"]})
