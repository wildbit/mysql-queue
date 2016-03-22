(ns mysql-queue.utils)

(defmacro while-let
  "Repeatedly executes body while test expression is true, evaluating the body with binding-form bound to the value of test."
  [[form tst] & body]
  `(loop [temp# ~tst]
     (when temp#
       (let [~form temp#]
         ~@body
         (recur ~tst)))))

(defn fn-options
  "Combines a naive list of options accepted by a function that
   uses destructuring for keyword arguments."
  [f-var]
  (->> f-var
       meta
       :arglists
       (map last)
       (filter map?)
       (mapcat :keys)
       (map keyword)
       (into #{})))

(defmacro with-error-handler
  "Returns a function of no arguments that calls fn with an exception object if one is thrown."
  [[f :as bindings] & body]
  {:pre [(= 1 (count bindings))]}
  `(fn []
     (try
       ~@body
       (catch Exception e#
         (~f e#)))))

