(ns #^{:doc "Entity models and relationships for the Google App Engine datastore service." }
  appengine.datastore.erm
  (:import (com.google.appengine.api.datastore 
	    EntityNotFoundException Query Query$FilterOperator Text))
  (:require [appengine.datastore.core :as ds])
  (:import (com.google.appengine.api.datastore
            DatastoreServiceFactory DatastoreServiceConfig
            Entity Key Query KeyFactory Transaction))
  (:use [clojure.contrib.string :only (join)]
        [clojure.contrib.seq :only (includes?)]
        appengine.utils	inflections))

(defn serialize [object] (binding [*print-dup* true] (pr-str object)))
(defn deserialize [data] (with-in-str data (read)))

(defn entity-obj->map
  "Converts a com.google.appengine.api.datastore.Entity instance to a
entity record (if it exists). The properties of the entity are stored
under their keyword names, the entity kind under :kind and the entity
key under :key.

Examples:

   (entity->map (doto (Entity. \"continent\") (.setProperty \"name\" \"Europe\")))
   ; => {:name \"Europe\", :kind \"continent\", :key #<Key continent(no-id-yet)>}"
  [entity]
  (let [kind               (.getKind entity)
        entity-record-name (ns-resolve *ns* (symbol kind))
        con                (first (.getDeclaredConstructors entity-record-name))
        num                (alength (.getParameterTypes con))
        entity-record      (.newInstance con (make-array Object num))]
    (reduce #(assoc %1 (keyword (key %2)) (val %2))
      (merge entity-record {:kind kind :key (.getKey entity)})
      (.entrySet (.getProperties entity)))))

(defmacro empty-record
  "Dynamic factory for defrecords."
  [name]
    `(let [con# (first (.getDeclaredConstructors ~name))
           num# (alength (.getParameterTypes con#))]
      (.newInstance con# (make-array Object num#))))

(defmacro entity
  ([name] `(entity ~name {}))
  ([name vals-map] `(assoc (create-entity (empty-record ~name) ~vals-map) :kind ~(str name))))

(defprotocol create-entity-protocol
  "Factory for creatiting entities with defaults."
  (create-entity [this vals-map] "Factory method for creating entities."))

(defprotocol process-entity-protocol
  "Process an entity before committing to datastore, or after retrieving."
  (preprocess [this] "Process entity before commit.")
  (postprocess [this] "Process entity after retrieve."))

(defrecord property-transform [pre post])

(defmacro deftransform [name pre-commit post-retrieve]
  `(def ~name (property-transform. ~pre-commit ~post-retrieve)))

(deftransform text-tr
  ; Pre-commit
  (fn [data] (Text. data))
  ; Post-retrieve
  (fn [data] (.getValue data)))

(deftransform serialize-tr
  ; Pre-commit
  (fn [data] (serialize data))
  ; Post-retrieve
  (fn [data] (deserialize data)))

(defn- entity-option-map [my-keyword option-map]
  (let [filtered-opts (filter #(contains? (second %) my-keyword) option-map)]
    (zipmap (keys option-map) (map my-keyword (vals option-map)))))

;; Core merge-with does not work with records as the first object.
;; The reason is that records do not support (my-map :key) access.
(defn rc-merge-with
  "Returns a map that consists of the rest of the maps conj-ed onto
  the first.  If a key occurs in more than one map, the mapping(s)
  from the latter (left-to-right) will be combined with the mapping in
  the result by calling (f val-in-result val-in-latter)."
  [f & maps]
  (when (some identity maps)
    (let [merge-entry (fn [m e]
			(let [k (key e) v (val e)]
			  (if (contains? m k)
			    (assoc m k (f (k m) v))
			    (assoc m k v))))
          merge2 (fn [m1 m2]
		   (reduce merge-entry (or m1 {}) (seq m2)))]
      (reduce merge2 maps))))

(defmacro defentity [entity [parent] attributes & options]
  (let [opt-map        (apply hash-map options)
        transform-map  (entity-option-map :transform opt-map)
        default-map    (entity-option-map :default opt-map)
        factory-fn     `(create-entity [this# vals-map#] (merge this# (merge ~default-map vals-map#)))
        pre-fn         `(preprocess [this#] (rc-merge-with (fn [v# t#] ((:pre t#) v#)) this# ~transform-map))
        post-fn        `(postprocess [this#] (rc-merge-with (fn [v# t#] ((:post t#) v#)) this# ~transform-map))]
    `(defrecord ~entity [~'kind ~'key ~@attributes]
      create-entity-protocol
      ~factory-fn
      process-entity-protocol
      ~pre-fn
      ~post-fn)))

;;  Examples:
; user=> (defentity citation [] [pmid abstract volume issue year month pages journal journal-abbrev authors]
;   :abstract {:transform text-tr :default ""}
;   :authors {:transform serialize-tr})
; user=> (entity citation)
; {:pmid nil,
;  :abstract "",
;  :volume nil,
;  :issue nil,
;  :year nil,
;  :month nil,
;  :pages nil,
;  :journal nil,
;  :journal-abbrev nil,
;  :authors nil}
; user=> entry
; {:abstract "Lorum ipsum...",
;  :authors ["Joe" "Jim" "Bob"],
;  :title "A title",
;  :year 2010}
; user=> (entity citation entry)
; {:pmid nil,
;  :abstract "Lorum ipsum...",
;  :volume nil,
;  :issue nil,
;  :year 2010,
;  :month nil,
;  :pages nil,
;  :journal nil,
;  :journal-abbrev nil,
;  :authors ["Joe" "Jim" "Bob"],
;  :title "A title"}
; user=> (preprocess (entity citation entry))
; {:pmid nil,
; 	 :abstract #<Text <Text: Lorum ipsum...>>,
; 	 :volume nil,
; 	 :issue nil,
; 	 :year 2010,
; 	 :month nil,
; 	 :pages nil,
; 	 :journal nil,
; 	 :journal-abbrev nil,
; 	 :authors "[\"Joe\" \"Jim\" \"Bob\"]",
; 	 :title "A title"}
; user=> (postprocess (preprocess (entity citation entry)))
; 	{:pmid nil,
; 	 :abstract "Lorum ipsum...",
; 	 :volume nil,
; 	 :issue nil,
; 	 :year 2010,
; 	 :month nil,
; 	 :pages nil,
; 	 :journal nil,
; 	 :journal-abbrev nil,
; 	 :authors ["Joe" "Jim" "Bob"],
; 	 :title "A title"}
	
;;  Planned for relationships:
; 	(defentity citation [] [...])
; 	(defentity author [] [...])
; 	(defrel many-to-many citation author)
; 	(selectrel citations my-author where (= :title "On queries"))
; 	(select-cursor citation filter-by (= :title "On queries") sort-by :year :month :day limit 20)
; 
; 	(defentity blog [] [...])
; 	(defentity post [] [...])
; 	(defrel many-to-one post blog)
; 	(selectrel posts my-blog where (= :tag "sql") limit 40)
; 
