(ns mysql-queue.utils-test
  (:require [mysql-queue.utils :refer :all]
            [clojure.test :refer :all]))

(deftest while-let-test
  (let [s (atom [1 2 3 4 5])]
    (is (nil? (while-let [[x] (seq @s)] (swap! s rest))))
    (is (empty? @s))))

(deftest ns->ms-test
  (is (= 0 (ns->ms 0)))
  (is (= 1 (ns->ms 1000000))))

(deftest profile-test
  (let [p (promise)
        [ret t] (profile (deref p 50 42))]
    (is (= 42 ret))
    (is (<= 50000000 t))))

(deftest profile-block-test
  (let [p (promise)
        [ret m] (profile-block [m]
                  (meter m :subop (deref p 50 nil))
                  (Thread/sleep 50)
                  42)]
    (is (= 42 ret))
    (is (<= 50000000 (:subop m) 60000000))
    (is (<= 100000000 (:full m)))))

