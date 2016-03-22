# mysql-queue

A Clojure library that implements a MySQL-backed durable queue with support for scheduled jobs.

## Why

Database-backed queues are known to be fraught with various problems.
It'd be unwise to deny this: compared to specialized queue and messaging systems they're slow and inefficient.
If one's looking at processing hundreds of jobs per second, it is certainly advisable to consider a specialized solution.
For smaller projects though, using a relational database as a queue offers considerable benefits:

* **No additional infrastructure requirements.** Most web applications already have a database.
* **Centralized queue.** Compared to embeddable solutions, you can easily add more workers running on different servers.
* **Scheduled jobs.** Specialized queue and messaging systems tend to not support scheduled messages, because they're simply not built for storing data. On the contrary, any in-database queue is inherently great at storing and filtering data.

The latter point is also important if what you're after is just a scheduler to push jobs to your internal queue.
For example, you may decide to use a database table to store scheduled jobs that will later be executed via Zach Tellman's [durable-queue](https://github.com/Factual/durable-queue).
Nevertheless, relegating your database-backed queue to a position of a scheduler doesn't make it less of a queue.
It still has to grapple with all of the concomitant issues, such as:

* **State management.** It is far from trivial to represent jobs' states using semantics of traditional relational database operations.
* **Concurrency.** Unless there is only a single worker process running at any time, you're going to need some sort of locking functionality that will ensure that only one worker gets a particular job.
* **Recovery.** Sometimes a worker fails without acknowledging successful job execution or the entire process crashes. Unless you're comfortable with dropping those messages, a recovery mechanism is required.
* **Graceful shutdown.** Even with "real" queues and messaging systems this important part is often overlooked and has to be implemented by the client application.
* **Logging and error handling.** Quick home-grown solutions tend to cut on logging, and often complicate debugging when something going wrong.

The `mysql-queue` library aims at providing a sane implementation of a MySQL-backed queue while addressing all of the issues above.
It is especially useful for compact monolith web applications requiring both scheduled and real-time message processing (at a smaller scale).
In real world, this library is running at the core of the free DMARC monitoring tool by Postmark responsible for sending weekly digests to the subscribers, as well as executing a handful of other repeating tasks.

## Usage

All public functions are located in the `mysql-queue.core` namespace:

``` clojure
(require '[mysql-queue.core :as queue])

(def db-conn {:subprotocol "mysql"
              :subname "//localhost:3306/myapp"
              :user "user"
              :password "password"})
```

This lib uses two MySQL tables: `jobs` and `scheduled_jobs`. The names are not configurable.
You can create these tables when your application starts by calling `initialize!` function.

``` clojure
; The following call is idempotent, meaning it will not attempt to create the tables if they already exist
(queue/initialize! db-conn)
```

Schedule jobs via `schedule-job` function:

``` clojure
(queue/schedule-job db-conn :my-job :begin {:echo "Hello, world!"} (java.util.Date.))
```

Jobs are functions of two arguments: `status` (Clojure keyword) and `args` (anything serializable with EDN).
A job function is expected to return a vector of two elements: new status and (potentially) updated args.

Examples: `[:done {}]`, `[:step2 {:step1-result 42}]`.

If a job returns something that isn't a vector of 2 elements, the return value considered to be `[:done nil]`.
The job is considered completed if the returned status is `:done`.
Two synonym statuses for `:done` are available to provide more context in logs: `:canceled` and `:failed`.
If the returned status isn't `:done`, it will be persisted and the job function will be executed again with returned arguments.
See Advanced section for more details on this behavior.

``` clojure
; Job function example
(defn my-job
  [status {:keys [echo] :as args}]
  (println echo)
  [:done args])
```

Symbolic job names passed to `schedule-job` are associated with job functions via a user-defined bindings map:

``` clojure
(def jobs {:my-job my-job})
```

Use `worker` function to create a worker that will periodically check the database for new jobs and execute
associated job functions on a thread pool. See docstring for advanced configuration options such as concurrency
settings and user-defined logging functions.

``` clojure
(def worker (queue/worker db-conn jobs))
```

To gracefully stop a worker, use `stop` function.
It will immediately stop publisher and recovery threads, and will then wait for the worker threads to process all loaded jobs for up to a specified number of seconds before unblocking the calling thread.

``` clojure
(queue/stop worker 5)
```

This covers the basics. Proceed to Advanced section for more details.

## Advanced

### Failproof job functions

Job functions used by `mysql-queue` are simple state machines. If your job is a combination of side effects (such as write to a database, notify an external service, then send an email), you may need to be able to pick up the job exactly where it left off in case any one of these multiple steps fails.

Take a look at the following job function:

``` clojure
(defn multi-step-job
  [status {data :data}]
  (->> data write-to-db (send-email :new-item) save-message-id))
```

This (valid) job function is quite terse, but still easy to understand if you're familiar with Clojure idioms.
Unfortunately, it couples three dependent side effects together into a single operation.
Assume there is a connection error to your mail transfer agent when sending that email.
It means that a given piece of data was recorded to the database, but the email wasn't yet sent.
If we retry this job, the data will be written to the database again, resulting in a duplicate.
Even worse, if `save-message-id` throws a database connection error, retrying this job will mean sending another notification.
To mitigate this problem, we could structure our job in a bit different way:

``` clojure
(defmulti multi-step-job (fn [status _] status))

(defmethod multi-step-job :begin
  [_ {data :data}]
  (let [id (write-to-db data)]
    [:send-email {:id id}]))

(defmethod multi-step-job :send-email
  [_ {id :id}]
  (let [message-id (send-email :new-item id)]
    [:save-message-id {:message-id id}]))

(defmethod multi-step-job :save-message-id
  [_ {:keys [message-id]}]
  (save-message-id message-id)
  [:done {}])
```

Here we used multimethods to represent all stateful transitions in our job.
It doesn't look nearly as concise as before, but the new representation reveals how many actions that threading macro was hiding.
Since the library persists any results returned by the job function, even if your VM crashes in the middle of the job, the recovery will later be able to pick up from the last successfully acknowledged step.

**Warning:** Be careful about returning large chunks of data because all job arguments have to be serialized and stored in the database.

## License

Copyright © 2016 Wildbit

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

