(ns mysql-queue.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.jdbc :as sql]
            [clojure.set :as clj-set]
            [clojure.string :as string]
            [mysql-queue.core :refer :all]
            [mysql-queue.queries :as queries]))

(Thread/setDefaultUncaughtExceptionHandler
  (reify Thread$UncaughtExceptionHandler
    (uncaughtException [_ thread throwable]
      (println "WARNING!!! Uncaught exception in core async:")
      (println throwable))))

(def db-conn {:subprotocol "mysql"
              :subname "//localhost:3306/clj_mysql_queue?useSSL=false"
              :user "root"
              :password ""})

(defn delete-scheduled-jobs-by-name!
  [db-conn job-name]
  (sql/delete! db-conn :scheduled_jobs ["name = ?" job-name]))

(defn count-jobs
  [db-conn]
  (sql/query db-conn ["SELECT COUNT(*) AS c FROM jobs"] {:result-set-fn (comp :c first)}))

(defn queue-size
  [db-conn]
  (sql/query db-conn ["SELECT COUNT(*) AS c FROM scheduled_jobs"] {:result-set-fn (comp :c first)}))

(defn setup-db
  [f]
  (initialize! db-conn)
  (f))

(defn clean-up
  [f]
  (delete-scheduled-jobs-by-name! db-conn "test-foo")
  (delete-scheduled-jobs-by-name! db-conn "slow-job")
  (delete-scheduled-jobs-by-name! db-conn "quick-job")
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

(deftest unbalanced-parallel-job-processing-test
  (let [num-slow-jobs 1
        num-quick-jobs 5
        expected-slow-set (->> num-slow-jobs range (map inc) (into #{}))
        expected-quick-set (->> num-quick-jobs range (map inc) (into #{}))
        quick-success? (promise)
        slow-success? (promise)
        exception (promise)
        slow-check-ins (check-in-atom expected-slow-set slow-success?)
        quick-check-ins (check-in-atom expected-quick-set quick-success?)
        jobs {:quick-job (fn [status {id :id :as args}]
                           (swap! quick-check-ins conj id)
                           [:done args])
              :slow-job (fn [status {id :id :as args}]
                          (when (deref quick-success? 2000 false)
                            (swap! slow-check-ins conj id))
                          [:done args])}
        _ (dotimes [n num-slow-jobs]
            (schedule-job db-conn :slow-job :begin {:id (inc n)} (java.util.Date.)))
        _ (dotimes [n num-quick-jobs]
            (schedule-job db-conn :quick-job :begin {:id (inc n)} (java.util.Date.)))]
    (with-worker [wrk (worker db-conn
                              jobs
                              :prefetch 3
                              :num-consumer-threads 2
                              :err-fn #(deliver exception %)
                              :max-scheduler-sleep-interval 0.1)]
      (is (deref slow-success? 2000 false)
          (str "Failed to process 1 slow job and " num-quick-jobs
               " quick jobs in 2 seconds.\n"
               "Missing slow job IDs: " (clj-set/difference expected-slow-set
                                                            @slow-check-ins) "\n"
               "Missing quick job IDs: " (clj-set/difference expected-quick-set
                                                            @quick-check-ins) "\n"
               "Exception?: " (deref exception 0 "nope")))
      (is (= num-slow-jobs (count @slow-check-ins))
          "The number of executed slow jobs doesn't match the number of jobs queued.")
      (is (= num-quick-jobs (count @quick-check-ins))
          "The number of executed quick jobs doesn't match the number of jobs queued."))))

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
              (queries/insert-job<! db-conn scheduled-id 0 "test-foo" "begin" (pr-str {:id (inc n)}) 1 (java.util.Date.))))]
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

(deftest stuck-job-max-attempts-test
  (let [jobs {:test-foo #(throw (Exception. "This job should not have been executed, because it reached max attempts."))}
        scheduled-id (schedule-job db-conn :test-foo :begin {} (java.util.Date. 0))]
    (queries/insert-job<! db-conn scheduled-id 0 "test-foo" "begin" (pr-str {}) 5 (java.util.Date. 0))
    (is (= 1 (count (queries/select-n-stuck-jobs db-conn ultimate-job-states ["test-foo"] [0] 5 5))))
    (is (= 1 (count-jobs db-conn)))
    (with-worker [wrk (worker db-conn
                              jobs
                              :num-consumer-threads 1
                              :max-scheduler-sleep-interval 0.5
                              :max-recovery-sleep-interval 0.5)]
      (Thread/sleep 1000)
      (is (zero? (count (queries/select-n-stuck-jobs db-conn ultimate-job-states ["test-foo"] [0] 5 5))))
      (is (zero? (count-jobs db-conn))))))

(deftest job-timeout-test
  (let [jobs {:test-foo (fn [_ _] (Thread/sleep 10000) [:done {}])}]
    (schedule-job db-conn :test-foo :begin {} (java.util.Date.))
    (with-worker [wrk (worker db-conn
                              jobs
                              :num-consumer-threads 1
                              :max-scheduler-sleep-interval 0.01
                              :max-recovery-sleep-interval 0.01
                              :recovery-threshold-mins (/ 1 60)
                              :job-timeout-mins (/ 1 60))]
      (Thread/sleep 10000)
      (is (zero? (count-jobs db-conn)))
      (is (zero? (queue-size db-conn))))))

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

(deftest status-test
  (let [num-jobs 100
        expected-set (->> num-jobs range (map inc) (into #{}))
        success? (promise)
        exception (promise)
        check-ins (check-in-atom expected-set success?)
        jobs {:test-foo (fn [status {id :id :as args}]
                          (Thread/sleep 20)
                          (swap! check-ins conj id)
                          [:done args])}]
    (with-worker [wrk (worker db-conn
                              jobs
                              :num-consumer-threads 1
                              :err-fn #(deliver exception %)
                              :max-scheduler-sleep-interval 0.5
                              :max-recovery-sleep-interval 2)]
      ; Initial status
      (let [{{:keys [scheduled-jobs jobs]} :db-queue
             :keys [consumers prefetched-jobs]}
            (status wrk)]
        (is (= 1 (count consumers)))
        (is (zero? (:overdue scheduled-jobs)))
        (is (zero? (:total scheduled-jobs)))
        (is (zero? (:stuck jobs)))
        (is (zero? (:total jobs)))
        (is (empty? prefetched-jobs)))

      ; Publishing jobs
      (dotimes [n num-jobs]
        (schedule-job db-conn :test-foo :begin {:id (inc n)} (java.util.Date.)))

      ; Publishing one stuck job
      (let [scheduled-id (schedule-job db-conn :test-foo :begin {:id 1} (java.util.Date. 0))]
        (queries/insert-job<! db-conn scheduled-id 0 "test-foo" "begin" (pr-str {:id 1}) 1 (java.util.Date. 0)))

      ; Status after publishing
      (let [{{:keys [scheduled-jobs jobs]} :db-queue
             :keys [consumers]
             :as status}
            (status wrk)]
        (is (= 1 (count consumers)))
        (is (<= 1 (:overdue scheduled-jobs) 101))
        (is (<= 1 (:total scheduled-jobs) 101))
        (is (<= 0 (:stuck jobs) 1))
        (is (<= 0 (:total jobs) 50)))

      (is (deref success? 15000 false) "Failed to process jobs in time. Test results below depend on this.")

      ; Extra sleep time to let stuck job processing finish
      (Thread/sleep 1000)

      ; Status after finished
      (let [{{:keys [scheduled-jobs jobs]} :db-queue
             :keys [consumers recent-jobs recent-jobs-stats]
             :as status}
            (status wrk)]
        (is (= 1 (count consumers)))
        (is (zero? (:overdue scheduled-jobs)))
        (is (zero? (:total scheduled-jobs)))
        (is (zero? (:stuck jobs)))
        (is (zero? (:total jobs)))
        (is (= 50 (count recent-jobs)))
        (is (= 50 (get-in recent-jobs-stats [:job-types :test-foo])))
        (is (= 101 (:jobs-executed (first consumers))))))))

