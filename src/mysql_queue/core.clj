(ns mysql-queue.core
  "A MySQL-backed durable queue implementation with scheduled jobs support."
  (:require [mysql-queue.queries :as queries]
            [mysql-queue.utils :refer [while-let fn-options with-error-handler profile-block meter ns->ms]]
            [clojure.string :as string]
            [clojure.set :as clj-set]
            [clojure.edn :as edn]
            [clojure.core.async :as async :refer [chan >!! >! <! <!! go go-loop thread thread-call close! timeout alts! alts!!]]
            [clojure.core.async.impl.protocols :as async-proto :refer [closed?]])
  (:import (com.mysql.jdbc.exceptions.jdbc4 MySQLIntegrityConstraintViolationException)
           (java.util Date)))

(def ultimate-job-states #{:canceled :failed :done})
(def max-retries 5)

(defn- job-summary-string
  [{status :status j-name :name id :id :as job}]
  (str (.getSimpleName (class job)) "[" id ":" (name j-name) ":" (name status) "]"))

(defprotocol Stoppable
  (stop [worker timeout-secs]))

(defrecord Worker
  [db-conn input-chan status sieve status-threads consumer-threads scheduler-thread recovery-thread options]
  Stoppable
  (stop [this timeout-secs]
    (when (:running @status)
      (swap! status assoc :running false)
      (close! input-chan)
      (let [consumer-shutdowns (->> consumer-threads
                                    (concat [scheduler-thread recovery-thread])
                                    async/merge
                                    (async/into []))
            [v ch] (alts!! [(timeout (* 1000 timeout-secs)) consumer-shutdowns])]
        (= ch consumer-shutdowns)))))

(defprotocol ToDb (*->db [entity]))

(defprotocol Persistent
  (persist [entity conn])
  (cleanup [entity conn]))

(defprotocol Executable
  (finished? [job])
  (execute [job db-conn log-fn err-fn]))

(defprotocol Fertile
  (beget [parent] [parent status] [parent status parameters]))

(defrecord Job [user-fn id scheduled-job-id parent-id name status parameters attempt]
  Object
  (toString [this]
    (job-summary-string this))
  ToDb
  (*->db [this]
    [scheduled-job-id
     parent-id
     (clojure.core/name name)
     (clojure.core/name status)
     (pr-str parameters)
     attempt])
  Persistent
  (persist [this conn]
    (if id
      this
      (try
        (let [{id :generated_key} (apply queries/insert-job<! conn (*->db this))]
          (assoc this :id id))
        (catch MySQLIntegrityConstraintViolationException e nil))))
  (cleanup [this conn]
    (when scheduled-job-id
      (queries/delete-scheduled-job-by-id! conn scheduled-job-id)
      nil))
  Fertile
  (beget [this] (beget this status parameters))
  (beget [this status] (beget this status parameters))
  (beget [this status parameters]
    (assoc this
           :id nil
           :parent-id id
           :status status
           :parameters parameters
           :attempt (if (= status (:status this)) (inc attempt) 1))))

(defn- job
  "Creates a new Job (or fn-constructor) record from a JDBC map representing a row of jobs table."
  ([db-row fn-bindings]
   (job db-row fn-bindings ->Job))
  ([db-row fn-bindings fn-constructor]
   (fn-constructor (fn-bindings (-> db-row :name keyword))
                   (:id db-row)
                   (:scheduled_job_id db-row)
                   (:parent_id db-row)
                   (-> db-row :name keyword)
                   (-> db-row :status keyword)
                   (-> db-row :parameters (String. "UTF-8") edn/read-string)
                   (:attempt db-row))))

(defrecord StuckJob [user-fn id scheduled-job-id parent-id name status parameters attempt]
  Object
  (toString [this]
    (job-summary-string this))
  Fertile
  (beget [this]
    (->Job user-fn nil scheduled-job-id id name status parameters (inc attempt)))
  (beget [this _] (beget this))
  (beget [this _ _] (beget this)))

(defn- stuck-job
  [db-row fn-bindings]
  (job db-row fn-bindings ->StuckJob))

(defrecord ScheduledJob [user-fn id name status parameters due-at]
  Object
  (toString [this]
    (job-summary-string this))
  ToDb
  (*->db [this]
    [(clojure.core/name name)
     (clojure.core/name status)
     (pr-str parameters)
     due-at])
  Persistent
  (persist [this conn]
    (if id
      this
      (let [{id :generated_key} (apply queries/insert-scheduled-job<! conn (*->db this))]
        (assoc this :id id))))
  (cleanup [this conn]
    (queries/delete-scheduled-job-by-id! conn id)
    nil)
  Fertile
  (beget [this] (beget this status parameters))
  (beget [this status] (beget this status parameters))
  (beget [this status parameters]
    (->Job user-fn nil id 0 name status parameters 1)))

(defn- scheduled-job
  [db-row fn-bindings]
  (->ScheduledJob (fn-bindings (-> db-row :name keyword))
                  (:id db-row)
                  (-> db-row :name keyword)
                  (-> db-row :status keyword)
                  (-> db-row :parameters (String. "UTF-8") edn/read-string)
                  (:scheduled_for db-row)))

(defn- job-result-or-nil
  "Returns its value if it's a valid job result or nil.
   Job result is a vector of two elements where the first element is a keyword."
  [result]
  (when (and (vector? result) (= 2 (count result)) (keyword? (first result)))
    result))

(extend-protocol Executable
  Job
  (finished? [job]
    (ultimate-job-states (:status job)))
  (execute [{:as job job-fn :user-fn :keys [status parameters attempt]} db-conn log-fn err-fn]
    (profile-block [m]
      (if (finished? job)
        (cleanup job db-conn)
        (try
          (log-fn :info job "Executing job " job)
          (let [[status params] (-> (meter m :job-fn (job-fn status parameters))
                                    job-result-or-nil (or [:done nil]))]
            (-> job (beget status params) (persist db-conn)))
          (catch Exception e
            (err-fn e)
            (if (< attempt max-retries)
              (-> job beget (persist db-conn))
              (-> job (beget :failed) (persist db-conn))))))))
  StuckJob
  (finished? [job] false)
  (execute [job db-conn log-fn err-fn]
    (profile-block [_]
      (log-fn :info job "Recovering job " job)
      (-> job beget (persist db-conn))))
  ScheduledJob
  (finished? [job]
    (throw (UnsupportedOperationException. "finished? is not implemented for ScheduledJob.")))
  (execute [job db-conn log-fn _err-fn]
    (profile-block [_]
      (log-fn :info job "Executing job " job)
      (-> job beget (persist db-conn)))))

(defn- get-scheduled-jobs
  "Searches for ready scheduled jobs and attempts to insert root jobs for each of those.
   Returns the number of jobs added, or false if channel was closed."
  [db-conn n fn-bindings sieve]
  (->> (queries/select-n-ready-scheduled-jobs db-conn (map name (keys fn-bindings)) sieve n)
       (map #(scheduled-job % fn-bindings))))

(defn- get-stuck-jobs
  "Searches DB for long-running jobs and enqueues follow-up jobs for them.
   Should not be run too often or the channel will be clogged
   with stuck IDs. Also keep in mind the number of concurrently running nodes."
  [db-conn n fn-bindings threshold sieve]
  (->> (queries/select-n-stuck-jobs db-conn
                                    (map name ultimate-job-states)
                                    (map name (keys fn-bindings))
                                    sieve
                                    threshold
                                    n)
       (map #(stuck-job % fn-bindings))))

(defn- batch-publish
  "Puts gitven jobs onto the channel chan and returns the number of successfully
   published jobs or false if the channel is closed."
  [publish-chan jobs]
  (let [total (->> jobs (take-while #(>!! publish-chan %)) count)]
    (if (or (not (async-proto/closed? publish-chan)) (pos? total))
      total
      false)))

(defn- default-status
  "Build the default status map for a given number of consumer threads."
  [{:keys [num-consumer-threads
           recovery-threshold-mins
           min-scheduler-sleep-interval
           max-scheduler-sleep-interval
           min-recovery-sleep-interval
           max-recovery-sleep-interval]}]
  {:running true
   :consumers (mapv #(hash-map :n (inc %)
                               :started-at (Date.)
                               :jobs-executed 0N
                               :recent-jobs [])
                    (range num-consumer-threads))
   :recovery {:started-at (Date.)
              :min-interval min-recovery-sleep-interval
              :max-interval max-recovery-sleep-interval
              :iterations 0N
              :jobs-published 0N}
   :scheduler {:started-at (Date.)
               :min-interval min-scheduler-sleep-interval
               :max-interval max-scheduler-sleep-interval
               :recovery-threshold-mins recovery-threshold-mins
               :iterations 0N
               :jobs-published 0N}})

(defn- consumer-thread
  "Consumer loop. Automatically quits if the listen-chan is closed. Runs in a go-thread."
  [id listen-chan status-chan db-conn log-fn err-fn]
  (go
    (while-let [job (<! listen-chan)]
      (try
        (log-fn :debug job "Consumer received job " job)
        (loop [current-job job
               metrics {}]
          (if current-job
            (do
              (>! status-chan {:id id :state :running-job :job current-job})
              (let [[next-job dmetrics] (<! (thread (execute current-job db-conn log-fn err-fn)))]
                (recur next-job (merge-with + metrics dmetrics))))
            (do
              (>! status-chan {:id id :state :finished-job :job current-job :metrics metrics})
              (log-fn :info current-job "Completed job " job " in " (ns->ms (:full metrics)) "ms")
              nil)))
        (catch Exception e
          (>! status-chan {:id id :state :error :job job})
          (log-fn :error job "Unexpected error " e " in consumer loop when running job " job)
          (err-fn e))))
    (>! status-chan {:id id :state :quit})
    (log-fn :debug "Consumer Thread" "Consumer is stopping...")
    :done))

(defn- publisher-thread
  "Publisher loop. Automatically quits if the publish-chan is closed. Runs in a go-thread."
  [status-chan min-sleep-secs max-sleep-secs source-fn log-fn]
  (go-loop [last-exec (System/currentTimeMillis)]
    (if-let [published (<! (thread-call source-fn))]
      (do
        (>! status-chan {:state :running :jobs-published published})
        (if (zero? published)
          (<! (timeout (max (* 1000 min-sleep-secs)
                            (- (* 1000 max-sleep-secs)
                               (- (System/currentTimeMillis) last-exec)))))
          (log-fn :debug nil "Published " published " new jobs."))
        (recur (System/currentTimeMillis)))
      (do
        (>! status-chan {:state :quit :jobs-published 0})
        (log-fn :debug nil "Publisher is stopping...")
        :done))))

(defn- status-threads
  "Status loop. Listens for status updates from consumer and publisher threads.
   Runs multiple go-threads."
  [status num-kept-jobs]
  (let [consumer-chan (chan)
        scheduler-chan (chan)
        recovery-chan (chan)
        consumer-fn (fn [{:keys [recent-jobs] :as status} consumer-state job metrics]
                      (let [current-time (Date.)
                            finished? (= consumer-state :finished-job)
                            overflow (inc (- (count recent-jobs) num-kept-jobs))
                            overflow? (pos? overflow)
                            job (assoc job :metrics metrics :processed-at current-time)]
                        (cond-> status
                          true (assoc :last-update-at current-time
                                      :last-job job
                                      :state consumer-state)
                          (and finished? overflow?) (update :recent-jobs subvec overflow)
                          finished? (update :jobs-executed inc)
                          finished? (update :recent-jobs conj job))))
        consumer-thread (go
                          (while-let [{:keys [id job state metrics]} (<! consumer-chan)]
                            (swap! status update-in [:consumers id] consumer-fn state job metrics)))
        publisher-fn (fn [status publisher-state jobs-published]
                       (-> status
                           (assoc :last-update-at (Date.)
                                  :state publisher-state
                                  :jobs-published-last-run jobs-published)
                           (update :jobs-published + jobs-published)
                           (update :iterations inc)))
        scheduler-thread (go
                           (while-let [{:keys [state jobs-published]} (<! scheduler-chan)]
                             (swap! status update :scheduler publisher-fn state jobs-published)))
        recovery-thread (go
                          (while-let [{:keys [state jobs-published]} (<! recovery-chan)]
                            (swap! status update :recovery publisher-fn state jobs-published)))]
    {:consumer {:channel consumer-chan :thread consumer-thread}
     :scheduler {:channel scheduler-chan :thread scheduler-thread}
     :recovery {:channel recovery-chan :thread recovery-thread}}))

(defn- sieve->ids
  "Returns a sieve seq that can be used to filter SQL queries for
   certain job types. Includes a 0 id to simplify the case when the
   sieve is empty."
  [sieve sieved-type]
  (->> sieve
       (clj-set/select #(instance? sieved-type %))
       (map :id)
       (concat [0])))

(defn- deduplicate
  "Takes a channel and returns a new input channel and n-outs output channels.
   The returned pipeline is deduplicated via an in-memory sieve of currently
   processed elements."
  [ch n-outs]
  (let [sieve (atom #{})
        in-ch (chan)
        out-chs (vec (repeatedly n-outs chan))]
    (go-loop []
      (if-let [v (<! in-ch)]
        (do
          (when-not (@sieve v)
            (swap! sieve conj v)
            (>! ch v))
          (recur))
        (close! ch)))
    (go-loop [occupations {}]
      (if-let [v (<! ch)]
        (let [[v ch] (alts! (map #(vector %1 v) out-chs))]
          (when-let [occupation (occupations ch)]
            (swap! sieve disj occupation))
          (recur (assoc occupations ch v)))
        (doseq [out-ch out-chs]
          (close! out-ch))))
    [in-ch out-chs sieve]))

(defn- quiet-log-fn
  "Returns a logging function that never throws an exception."
  [f]
  (fn [level job & parts]
    (try
      (f level job (apply str parts))
      (catch Exception e
        (println "Unexpected exception in user log-fn: " e)))))

(defn- quiet-err-fn
  "Returns an error handling function that never throws an exception."
  [f]
  (fn [e]
    (try
      (f e)
      (catch Exception e
        (println "Unexpected exception in user err-fn: " e)))))

(defn- publisher-error-handler
  "Returns an error handler that logs with log-fn and reports via err-fn.
   Locus is used to additionally specify the context."
  [log-fn err-fn locus]
  (fn [^Exception e]
    (log-fn :error nil "Unexpected error " e " in " locus)
    (log-fn :error nil (->> e .getStackTrace (string/join "\n")))
    (err-fn e)
    0))

(defn initialize!
  "Create required MySQL tables if they don't exist. Returns true."
  [db-conn]
  (queries/create-scheduled-jobs! db-conn)
  (queries/create-jobs! db-conn)
  true)

(defn schedule-job
  "Creates a scheduled job with provided name, status, args and due time.
   Returns the unique numeric id of a created job."
  [db-conn name status params due-at]
  {:pre [(keyword? name)
         (keyword? status)]}
  (-> (->ScheduledJob nil nil name status params due-at)
      (persist db-conn)
      :id))

(defn worker
  "Creates a new worker. Takes a database connection db-conn,
   a map of fn-bindings binding job names to job functions, and a number
   of optional keyword arguments:
   
   * buffer-size - maximum number of jobs allowed into internal queue. Determines
     when the publisher will block. Default 10.
   * prefetch - the number of jobs a publisher fetches from the database at once.
     Default 10.
   * num-consumer-threads - the number of concurrent threads that run jobs at the
     same time.
   * min-scheduler-sleep-interval - the minimum time in seconds the scheduler will sleep
     before querying the database for due jobs. Default 0 seconds.
   * max-scheduler-sleep-interval - the maximum time in seconds the scheduler will sleep
     before querying the database for due jobs. Default 10 seconds.
   * min-recovery-sleep-interval - the minimum time in seconds the recovery thread will
     sleep before querying the database for stuck jobs. Default 0 seconds.
   * max-recovery-sleep-interval - the maximum time in seconds the recovery thread will
     sleep before qerying the database for stuck jobs. Default 10 seconds.
   * recovery-threshold-mins - the number of seconds after which a job is considered
     stuck and will be picked up by the recovery thread.
   * log-fn - user-provided logging function of 3 arguments: level (keyword), job (record), message (msg).
   * err-fn - user-provided error function of one argument: error (Exception)."
  [db-conn
   fn-bindings
   &{:keys [buffer-size
            prefetch
            num-stats-jobs
            num-consumer-threads
            min-scheduler-sleep-interval
            max-scheduler-sleep-interval
            min-recovery-sleep-interval
            max-recovery-sleep-interval
            recovery-threshold-mins
            log-fn
            err-fn]
     :or {buffer-size 10
          prefetch 10
          num-stats-jobs 50
          num-consumer-threads 2
          min-scheduler-sleep-interval 0
          max-scheduler-sleep-interval 10
          min-recovery-sleep-interval 0
          max-recovery-sleep-interval 10
          recovery-threshold-mins 20
          log-fn (constantly nil)
          err-fn (constantly nil)}
     :as options}]
  {:pre [(every? keyword? (keys fn-bindings))
         (every? fn? (vals fn-bindings))
         (every? (fn-options #'worker) (keys options))
         (fn? log-fn)
         (fn? err-fn)]}
  (let [log-fn (quiet-log-fn log-fn)
        err-fn (quiet-err-fn err-fn)
        options {:buffer-size buffer-size
                 :prefetch prefetch
                 :num-stats-jobs num-stats-jobs
                 :num-consumer-threads num-consumer-threads
                 :min-scheduler-sleep-interval min-scheduler-sleep-interval
                 :max-scheduler-sleep-interval max-scheduler-sleep-interval
                 :min-recovery-sleep-interval min-recovery-sleep-interval
                 :max-recovery-sleep-interval max-recovery-sleep-interval
                 :recovery-threshold-mins recovery-threshold-mins
                 :log-fn log-fn
                 :err-fn err-fn}
        status (atom (default-status options))
        {:as status-threads
         {consumer-status-channel :channel} :consumer
         {recovery-status-channel :channel} :recovery
         {scheduler-status-channel :channel} :scheduler} (status-threads status num-stats-jobs)
        queue-chan (chan buffer-size)
        [in-ch out-chs sieve] (deduplicate queue-chan num-consumer-threads)
        consumer-threads (->> out-chs
                              (map-indexed
                                #(consumer-thread %1 %2 consumer-status-channel db-conn log-fn err-fn))
                              (into [])
                              doall)
        handler (partial publisher-error-handler log-fn err-fn)
        scheduler-thread (publisher-thread scheduler-status-channel
                                           min-scheduler-sleep-interval
                                           max-scheduler-sleep-interval
                                           (with-error-handler [(handler "scheduler thread")]
                                             (batch-publish in-ch
                                                            (get-scheduled-jobs db-conn
                                                                                prefetch
                                                                                fn-bindings
                                                                                (sieve->ids @sieve ScheduledJob))))
                                           log-fn)
        recovery-thread (publisher-thread recovery-status-channel
                                          min-recovery-sleep-interval
                                          max-recovery-sleep-interval
                                          (with-error-handler [(handler "recovery thread")]
                                            (batch-publish in-ch
                                                           (get-stuck-jobs db-conn
                                                                           prefetch
                                                                           fn-bindings
                                                                           recovery-threshold-mins
                                                                           (sieve->ids @sieve StuckJob))))
                                          log-fn)]
    (log-fn :info nil "Starting a new worker...")
    (->Worker db-conn
              in-ch
              status
              sieve
              status-threads
              consumer-threads
              scheduler-thread
              recovery-thread
              options)))

