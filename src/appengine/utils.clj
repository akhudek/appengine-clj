(ns #^{:author "Roman Scherer"
       :doc "Utility functions for Google App Engine." }
  appengine.utils)

(defn compact [seq]
  (remove nil? seq))

(defn map-keys
  "Returns a lazy sequence consisting of the result of applying f to
  the keys of coll."
  [coll f] (zipmap (map f (keys coll)) (vals coll)))

(defn map-keyword [coll]
  "Returns a lazy sequence consisting of the result of applying
  #'keyword to the keys of coll."
  (map-keys coll #'keyword))

(defn keyword->string
  "Returns a string by calling str on the keyword and removing the
double colon at the beginning. If called with a string as argument the
string will be returned without modification.

Examples:

  (keyword->string :iso-3166-alpha-2)
  ; => \"iso-3166-alpha-2\"

  (keyword->string \"iso-3166-alpha-2\")
  ; => \"iso-3166-alpha-2\"
"
  [keyword]
  (let [string (str keyword)]
    (if (= (first string) \:) (apply str (rest string)) string)))

(defn stringify
  "Returns a stringified version of the given argument. Keywords are
converted with the keyword->string function, all other by calling str
on the argument.

Examples:

  (stringify \"iso-3166-alpha-2\")
  ; => \"iso-3166-alpha-2\"

  (stringify :iso-3166-alpha-2)
  ; => \"iso-3166-alpha-2\"

  (stringify 'iso-3166-alpha-2)
  ; => \"iso-3166-alpha-2\"
"
  [arg] (if (keyword? arg) (keyword->string arg) (str arg)))
