(ns mysql-queue.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.jdbc :as sql]
            [clojure.set :as clj-set]
            [clojure.string :as string]
            [mysql-queue.core :refer :all]
            [mysql-queue.queries :as queries]))

(def db-conn {:subprotocol "mysql"
              :subname "//localhost:3306/clj_mysql_queue"
              :user "root"
              :password ""})

(defn delete-scheduled-jobs-by-name!
  [db-conn job-name]
  (sql/delete! db-conn :scheduled_jobs ["name = ?" job-name]))

(defn setup-db
  [f]
  (queries/create-scheduled-jobs! db-conn)
  (queries/create-jobs! db-conn)
  (f))

(defn clean-up
  [f]
  (delete-scheduled-jobs-by-name! db-conn "test-foo")
  (f))

(use-fixtures :once setup-db)
(use-fixtures :each clean-up)

(defmacro with-worker
  [[bound-name expr :as args] & body]
  {:pre (= 2 (count args))}
  `(let [~bound-name ~expr]
     (try
       ~@body
       (finally
         (stop ~bound-name 2)))))

(defn check-in-atom [expected-set success-promise]
  (doto (atom [])
    (add-watch :test (fn [_ _ _ v]
                       (when (= (set v) expected-set)
                         (deliver success-promise true))))))

(deftest job-processing-test
  (let [num-jobs 100
        expected-set (->> num-jobs range (map inc) (into #{}))
        success? (promise)
        exception (promise)
        check-ins (check-in-atom expected-set success?)
        jobs {:test-foo (fn [status {id :id :as args}]
                          (Thread/sleep 10)
                          (swap! check-ins conj id)
                          [:done args])}
        _ (dotimes [n num-jobs]
            (schedule-job db-conn :test-foo :begin {:id (inc n)} (java.util.Date.)))]
    (with-worker [wrk (worker db-conn
                              jobs
                              :num-consumer-threads 1
                              :err-fn #(deliver exception %)
                              :max-scheduler-sleep-interval 0.5
                              :max-recovery-sleep-interval 0.5)]
      (is (deref success? 15000 false)
          (str "Failed to process " num-jobs " test jobs in 15 seconds.\n"
               "Missing job IDs: " (clj-set/difference expected-set @check-ins) "\n"
               "Exception?: " (deref exception 0 "nope")))
      (is (= num-jobs (count @check-ins))
          "The number of executed jobs doesn't match the number of jobs queued."))))

(deftest parallel-job-processing-test
  (let [num-jobs 100
        expected-set (->> num-jobs range (map inc) (into #{}))
        success? (promise)
        exception (promise)
        check-ins (check-in-atom expected-set success?)
        jobs {:test-foo (fn [status {id :id :as args}]
                          (Thread/sleep 10)
                          (swap! check-ins conj id)
                          [:done args])}
        _ (dotimes [n num-jobs]
            (schedule-job db-conn :test-foo :begin {:id (inc n)} (java.util.Date.)))]
    (with-worker [wrk (worker db-conn
                              jobs
                              :prefetch 4
                              :num-consumer-threads 4
                              :err-fn #(deliver exception %)
                              :max-scheduler-sleep-interval 0.5
                              :max-recovery-sleep-interval 0.5)]
      (is (deref success? 15000 false)
          (str "Failed to process " num-jobs " test jobs in 15 seconds.\n"
               "Missing job IDs: " (clj-set/difference expected-set @check-ins) "\n"
               "Exception?: " (deref exception 0 "nope")))
      (is (= num-jobs (count @check-ins))
          "The number of executed jobs doesn't match the number of jobs queued."))))

(deftest distributed-job-processing-test
  (let [num-jobs 100
        expected-set (->> num-jobs range (map inc) (into #{}))
        success? (promise)
        exception (promise)
        check-ins (check-in-atom expected-set success?)
        jobs {:test-foo (fn [status {id :id :as args}]
                          (Thread/sleep 10)
                          (swap! check-ins conj id)
                          [:done args])}
        _ (dotimes [n num-jobs]
            (schedule-job db-conn :test-foo :begin {:id (inc n)} (java.util.Date.)))]
    (with-worker [wrk-1 (worker db-conn
                                jobs
                              :prefetch 4
                              :num-consumer-threads 2
                              :err-fn #(deliver exception %)
                              :max-scheduler-sleep-interval 0.5
                              :max-recovery-sleep-interval 0.5)]
      (with-worker [wrk-2 (worker db-conn
                                  jobs
                                  :prefetch 4
                                  :num-consumer-threads 2
                                  :err-fn #(deliver exception %)
                                  :max-scheduler-sleep-interval 0.5
                                  :max-recovery-sleep-interval 0.5)]
        (is (deref success? 15000 false)
            (str "Failed to process " num-jobs " test jobs in 15 seconds.\n"
                 "Missing job IDs: " (clj-set/difference expected-set @check-ins) "\n"
                 "Exception?: " (deref exception 0 "nope")))
        (is (= num-jobs (count @check-ins))
            "The number of executed jobs doesn't match the number of jobs queued.")))))

(deftest stuck-job-processing-test
  (let [num-jobs 100
        expected-set (->> num-jobs range (map inc) (into #{}))
        success? (promise)
        exception (promise)
        check-ins (check-in-atom expected-set success?)
        jobs {:test-foo (fn [status {id :id :as args}]
                          (Thread/sleep 10)
                          (swap! check-ins conj id)
                          [:done args])}
        _ (dotimes [n num-jobs]
            (let [scheduled-id (schedule-job db-conn :test-foo :begin {:id (inc n)} (java.util.Date.))]
              (queries/insert-job<! db-conn scheduled-id 0 "test-foo" "begin" (pr-str {:id (inc n)}) 1)))]
    (with-worker [wrk (worker db-conn
                              jobs
                              :num-consumer-threads 1
                              :err-fn #(deliver exception %)
                              :recovery-threshold-mins 0
                              :max-scheduler-sleep-interval 0.5
                              :max-recovery-sleep-interval 0.5)]
      (is (deref success? 15000 false)
          (str "Failed to process " num-jobs " test jobs in 15 seconds.\n"
               "Missing job IDs: " (clj-set/difference expected-set @check-ins) "\n"
               "Exception?: " (deref exception 0 "nope")))
      (is (= num-jobs (count @check-ins))
          "The number of executed jobs doesn't match the number of jobs queued."))))

(deftest graceful-shutdown-test
  (let [num-jobs 2
        expected-set (->> num-jobs range (map inc) (into #{}))
        success? (promise)
        exception (promise)
        lock (promise)
        check-ins (check-in-atom expected-set success?)
        jobs {:test-foo (fn [status {id :id :as args}]
                          @lock
                          (Thread/sleep 1500)
                          (swap! check-ins conj id)
                          [:done args])}
        _ (dotimes [n num-jobs]
            (schedule-job db-conn :test-foo :begin {:id (inc n)} (java.util.Date.)))]
    (with-worker [wrk (worker db-conn
                              jobs
                              :num-consumer-threads 2
                              :err-fn #(deliver exception %)
                              :recovery-threshold-mins 0
                              :max-scheduler-sleep-interval 0.5
                              :max-recovery-sleep-interval 0.5)]
      (deliver lock :unlocked)
      (Thread/sleep 1000))
    (is (deref success? 10 false)
        (str "Failed to finish " num-jobs " test jobs taking 1500ms with 2s quit timeout.\n"
             "Missing job IDs: " (clj-set/difference expected-set @check-ins) "\n"
             "Exception?: " (deref exception 0 "nope")))
    (is (= num-jobs (count @check-ins))
        "The number of executed jobs doesn't match the number of jobs queued.")))

