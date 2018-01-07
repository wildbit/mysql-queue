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

(defn ns->ms
  "Converts a value in nanoseconds to milliseconds."
  [t]
  (Math/round (/ (double t) 1000000)))

(defmacro profile
  "Profiles a block of code. Returns a vector with original return value and
   elapsed time in ns."
  [& body]
  `(let [start# (System/nanoTime)
         ret# (do ~@body)
         elapsed# (- (System/nanoTime) start#)]
     [ret# elapsed#]))

(defmacro meter
  "Profiles a block of code. Assocs result to a named key in a provided atom.
   Returns the original return value."
  [metrics name & body]
  `(let [[ret# elapsed#] (profile ~@body)]
     (swap! ~metrics assoc ~name elapsed#)
     ret#))

(defmacro profile-block
  "Profiles a block of code with multiple named hot spots. Requires a binding for
   optionally used atom. Pass this atom to `meter` to register hot spots.
   Returns a map of hotspot => execution time (in ns).
   The entire block is wrapped in :full hotspot by default."
  [[metrics :as bindings] & body]
  {:pre [(= 1 (count bindings))]}
  `(let [metrics# (atom {})
         ~metrics metrics#
         ret# (meter metrics# :full ~@body)]
     [ret# (deref metrics#)]))

