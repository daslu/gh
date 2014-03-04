(ns gh.core
  (:use [plumbing.core :only [fnk for-map defnk]]
        [clojure.pprint]
        [clojure.data.priority-map]
        [clojure.repl]
        [clj-time.periodic]
        [incanter core stats charts io zoo]
        [clojure.algo.generic.functor :only [fmap]])
  (:require [plumbing.graph :as graph]
            [plumbing.fnk.pfnk :as pfnk]
            [clojure.java [io :as io] [shell :as shell]]
            [clojure.data.json :as json]
            [clj-time.core :as t]
            [clojure.stacktrace :as st])
  (:import java.util.zip.GZIPInputStream
           java.io.BufferedInputStream
           java.io.BufferedOutputStream
           java.io.FileOutputStream           
           java.io.File
           java.lang.Math
           java.io.InputStreamReader 
           java.io.BufferedReader))

(def ^:dynamic cache-path
  "the path for caching results of computations"
  "/home/we/workspace/data/gh/")

(defn memoize-fnk [f]
  (pfnk/fn->fnk (memoize f) (pfnk/io-schemata f)))

(defrecord Ymdh [year month day hour])

(defn
  ^{:doc "Construct the filaname of github archive data corresponding to given year, month, day and hour."
    :test (assert (= "2012-04-11-15.json.gz"
                     (construct-filename (Ymdh. 2012 04 11 15))))}
  construct-filename [ymdh]
  (str (:year ymdh)
       "-" (format "%02d" (:month ymdh))
       "-" (format "%02d" (:day ymdh))
       "-" (:hour ymdh)
       ".json.gz"))

(defn download-data [ymdh]
  (let [filename (construct-filename ymdh)
        path (str "/home/we/workspace/data/github-archive/"
                  filename)]
    (if (not (.exists (File. path)))
      (let [url (str "http://data.githubarchive.org/"
                     filename)]
        (println ["downloading" url])
        (shell/sh "wget" "-q" url "-O" path)))
    path))

(defn gunzip
  [fi fo]
  (with-open [i (io/reader
                 (java.util.zip.GZIPInputStream.
                  (io/input-stream fi)))
              o (java.io.PrintWriter. (io/writer fo))]
    (doseq [l (line-seq i)]
      (.println o l))))

(defn construct-data-reader [ymdh]
  
  )

(defn read-as-json [ymdh]
  (with-open [reader (-> ymdh
                         download-data
                         io/input-stream
                         GZIPInputStream.
                         io/reader)]
    (json/read-str (str "["
                        (clojure.string/replace
                         (apply str (line-seq reader))
                         "#}{" "#},{")
                        "]")
                   :key-fn keyword)))

(defn analyse [ymdh]
  (->> ymdh
       read-as-json
       (map (comp :language :repository))
       frequencies
       (into (priority-map))))

(defn date-time-to-ymdh [dt]
  (Ymdh. (t/year dt)
           (t/month dt)
           (t/day dt)
           (t/hour dt)))

(defn construct-ymdhs-seq
  [beginning-date-time]
  (map date-time-to-ymdh
       (periodic-seq beginning-date-time
                     org.joda.time.Hours/ONE)))

(def all-relevant-date-times
  (periodic-seq ;(t/date-time 2011 2 12)
   (t/date-time 2012 3 12)
   org.joda.time.Hours/ONE))

(comment
  (let [d (->> (construct-ymdhs-seq (t/date-time 2013))
               (take 1200)
               (pmap (fn [ymdh]
                       (into (select-keys (analyse ymdh)
                                          ["Java" "JavaScript"])
                             ymdh)))
               to-dataset)]
    (view
     (add-lines
      (xy-plot (range (nrow d))
               ($  "JavaScript" d))
      (range (nrow d))
      ($ "Java" d)))))

(comment
  (let [d (->> all-relevant-date-times
               (filter #(and (= 0 (t/hour %))
                             (= 1 (t/day-of-week %))))
               (map date-time-to-ymdh)
               (take 100)
               (pmap (fn [ymdh]
                       (into (select-keys (analyse ymdh)
                                          ["Java" "JavaScript" "Clojure"])
                             ymdh)))
               to-dataset)]
    (view
          (add-lines
           (add-lines
            (xy-plot (range (nrow d))
                     (map log ($  "JavaScript" d)))
            (range (nrow d))
            (map log ($ "Clojure" d)))
           (range (nrow d))
           (map log ($ "Java" d))))))

(comment
  (->> all-relevant-date-times
       (filter #(and (= 0 (t/hour %))
                     (= 1 (t/day-of-week %))))
       (map date-time-to-ymdh)
       (take 10)
       (mapcat read-as-json)
       (map :repository)
       (filter :url)
       (group-by :url)
       (map (comp last second))
       (map #(select-keys % [:watchers :forks :has_wiki :has_issues :language]))
       to-dataset
       ($where {:language {:$in #{"Perl" "Clojure" "Scala" "C"}}})
       (add-derived-column :log-forks [:forks] (comp log inc))
       (add-derived-column :log-watchers [:watchers] (comp log inc))
       (scatter-plot :log-forks :log-watchers
                     :group-by :language
                     :data)
       view)
)


(comment
  (let [events-set #{"WatchEvent" "PushEvent"}]    
    (->> all-relevant-date-times
         (filter #(and (= 0 (t/hour %))
                       (= 1 (t/day-of-week %))))
         (map date-time-to-ymdh)
         (take 30)
         (mapcat read-as-json)
         (map #(into (select-keys % [:type])
                     (select-keys (:repository %) [:language])))
         (filter (comp events-set
                       :type))
         frequencies
         (map (fn [[val count]]
                (into val
                      {:count count})))
         (group-by :language)
         (map (fn [[language event-counts]]
                (into {:language language}
                      (map (juxt (comp keyword
                                       :type)
                                 :count)
                           event-counts))))
         (filter #(and (= (inc (count events-set))
                          (count %))
                       (< 10 (apply min (vals (dissoc % :language))))))
         (map #(fmap (fn [val]
                       (if (number? val) (log val) val))
                     %))
         to-dataset
         (scatter-plot :WatchEvent :PushEvent
                       :data)
         view)))

(def so (read-dataset "/home/we/workspace/data/so.csv"
                      :header true))


(comment
  (let [x (->> so
               ($order [:y :m] :asc)
               ($where {:TagName "clojure"})
               ($ :c))
        y (->> so
               ($order [:y :m] :asc)
               ($where {:TagName "clojurescript"})
               ($ :c))]
    (view (add-lines
           (xy-plot (range (count x))
                    x)
           (range (count y))
           y))))
