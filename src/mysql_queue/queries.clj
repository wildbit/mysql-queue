(ns mysql-queue.queries
  (:require [clojure.java.jdbc :as sql]
            [clojure.string :as string]))

(defn create-scheduled-jobs!
  [db]
  (sql/execute! db
    ["CREATE TABLE IF NOT EXISTS `scheduled_jobs` (
       `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
       `name` varchar(255) COLLATE utf8_bin NOT NULL,
       `status` varchar(255) COLLATE utf8_bin NOT NULL DEFAULT 'running',
       `parameters` blob NOT NULL,
       `scheduled_for` datetime NOT NULL,
       `created_at` datetime NOT NULL,
       PRIMARY KEY (`id`),
       KEY `scheduled_jobs_by_scheduled_for` (`scheduled_for`),
       KEY `scheduled_jobs_by_name` (`name`)
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"]))

(defn create-jobs!
  [db]
  (sql/execute! db
    ["CREATE TABLE IF NOT EXISTS `jobs` (
       `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
       `scheduled_job_id` bigint(20) unsigned DEFAULT NULL,
       `parent_id` bigint(20) unsigned DEFAULT NULL,
       `name` varchar(255) COLLATE utf8_bin NOT NULL,
       `status` varchar(255) COLLATE utf8_bin NOT NULL DEFAULT 'running',
       `parameters` mediumblob NOT NULL,
       `attempt` int(10) unsigned NOT NULL DEFAULT '1',
       `created_at` datetime NOT NULL,
       PRIMARY KEY (`id`),
       UNIQUE KEY `jobs_by_scheduled_job_id_and_parent_id` (`scheduled_job_id`,`parent_id`),
       KEY `jobs_by_name` (`name`),
       KEY `jobs_by_status` (`status`),
       KEY `jobs_by_created_at` (`created_at`),
       CONSTRAINT `jobs_ibfk_1` FOREIGN KEY (`scheduled_job_id`) REFERENCES `scheduled_jobs` (`id`) ON DELETE CASCADE
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"]))

(defn in-query-stubs
  [xs]
  (string/join "," (repeat (count xs) "?")))

(defn insert-scheduled-job<!
  [db job-name job-status parameters due-at]
  (first (sql/insert! db :scheduled_jobs
                      {:name job-name
                       :status job-status
                       :parameters parameters
                       :scheduled_for due-at
                       :created_at (java.util.Date.)})))

(defn insert-job<!
  [db scheduled-job-id parent-id job-name job-status parameters attempt]
  (first (sql/insert! db :jobs
                      {:scheduled_job_id scheduled-job-id
                       :parent_id parent-id
                       :name job-name
                       :status job-status
                       :parameters parameters
                       :attempt attempt
                       :created_at (java.util.Date.)})))

(defn select-n-ready-scheduled-jobs
  [db jobs-names sieved-ids n]
  (sql/query db
    (concat [(str "SELECT
                     scheduled_jobs.*
                   FROM
                     scheduled_jobs LEFT JOIN
                     jobs ON scheduled_jobs.id=jobs.scheduled_job_id
                   WHERE
                     scheduled_jobs.scheduled_for <= ? AND
                     jobs.id IS NULL AND
                     scheduled_jobs.name IN (" (in-query-stubs jobs-names) ") AND
                     scheduled_jobs.id NOT IN (" (in-query-stubs sieved-ids) ")
                   LIMIT ?")]
            (concat [(java.util.Date.)] jobs-names sieved-ids [n]))))

(defn select-jobs-by-ids
  [db ids]
  (sql/query db
    (concat [(str "SELECT * FROM jobs WHERE id IN(" (in-query-stubs ids) ")")] ids)))

(defn select-n-stuck-jobs
  [db ultimate-statuses job-names sieved-ids threshold-mins n]
  (sql/query db
    (concat [(str "SELECT
                     jobs.*
                   FROM
                     jobs LEFT JOIN
                     jobs children ON jobs.id=children.parent_id
                   WHERE
                     children.id IS NULL AND
                     jobs.status NOT IN (" (in-query-stubs ultimate-statuses) ") AND
                     jobs.name IN (" (in-query-stubs job-names) ") AND
                     jobs.id NOT IN (" (in-query-stubs sieved-ids) ") AND
                     jobs.created_at + INTERVAL ? MINUTE <= ?
                   LIMIT ?")]
            ultimate-statuses
            job-names
            sieved-ids
            [threshold-mins (java.util.Date.) n])))

(defn delete-scheduled-job-by-id!
  [db id]
  (sql/delete! db :scheduled_jobs ["id=?" id]))

