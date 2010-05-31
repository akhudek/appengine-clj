(ns #^{:author "Roman Scherer"
       :doc "The entity API for the Google App Engine datastore service." }
  appengine.datastore.entities
  (:import (com.google.appengine.api.datastore 
	    EntityNotFoundException Query Query$FilterOperator Text))
  (:require [appengine.datastore.core :as ds])
  (:use [clojure.contrib.str-utils2 :only (join)]
        [clojure.contrib.seq-utils :only (includes?)]
        appengine.utils	inflections))


(defn- fn-name [prefix postfix] (symbol (str prefix "-" postfix)))

(defmulti preprocess-entity :kind)
(defmulti postprocess-entity :kind)

(defn- entity-option? [option entity-specs]
  (let [[attribute & options] entity-specs]
    (= (option (apply hash-map options)) true)))

(defn- entity-properties [entity-specs my-key]
  (map first (filter #(entity-option? my-key %) entity-specs)))

(defn- find-entities-fn-doc [entity property]
  (str "Find all " (pluralize (str entity)) " by " property "."))

(defn- find-entities-fn-name [entity property]
  (str "find-all-" (pluralize (str entity)) "-by-" property))

(defn- find-entity-fn-doc [entity property]
  (str "Find the first " entity " by " property "."))

(defn- find-entity-fn-name [entity property]
  (str "find-" entity "-by-" property))

(defn- filter-query [entity property value & [operator]]
  "Returns a query, where the value of the property matches using the
operator."
  (doto (Query. (str entity))
    (.addFilter
     (str property)
     (or operator Query$FilterOperator/EQUAL)
     (if (map? value) ((keyword value) value) value))))

(defn- serialize-property [prop entity]
	(let [property-key (keyword prop)]
		`[~property-key (Text. (serialize (~property-key ~entity)))]))

(defn- deserialize-property [prop entity]
	(let [property-key (keyword prop)]
		`[~property-key (deserialize (.getValue (~property-key ~entity)))]))

(defn- property->text [prop entity]
	(let [property-key (keyword prop)]
		`[~property-key (Text. (~property-key ~entity))]))		

(defn- text->property [prop entity]
	(let [property-key (keyword prop)]
		`[~property-key (.getValue (~property-key ~entity))]))

(defn- flatten-first-level [col]
	"Returns a list that is the concatenation of all the lists in col."
	(let [cnt (count col)]
  	(cond 
    	(= cnt 0) col
		  (= cnt 1) (first col)
		  :else     (reduce (fn [a b] (apply (partial conj a) b)) col))))

(defn filter-fn [entity property & [operator]]
  "Returns a filter function that returns all entities, where the
property matches the operator."
  (let [operator (or operator Query$FilterOperator/EQUAL)]
    (fn [property-val]
      (postprocess-entity	(ds/find-all (filter-query entity property property-val operator))))))

(defn- key-fn-name [entity]
  "Returns the name of the key builder fn for the given entity."
  (str "make-" (str entity) "-key"))

(defmacro def-key-fn [entity entity-keys & [parent]]
  "Defines a function to build a key from the entity-keys
propertoes. If entity-keys is empty the fn returns nil."
	`(defn ~(symbol (key-fn-name entity)) [~@(compact [parent]) ~'attributes]
  	~(if-not (empty? entity-keys)
    	`(ds/create-key
      		(:key ~parent)
        	~(str entity)
        	(join "-" [~@(map #(list (if (keyword? %) % (keyword %)) 'attributes) entity-keys)])))))

(defmacro def-make-fn [entity [parent] & properties]
  "Defines a function to build entity hashes."
  (let [args (compact [parent 'attributes])]
    `(defn ~(fn-name "make" entity) [~@args]       
       (merge (assoc (select-keys ~'attributes [~@(map (comp keyword first) properties)])
                :kind ~(str entity))
	      (let [key# (~(symbol (key-fn-name entity)) ~@args)]
		(if-not (nil? key#) {:key key#} {}))))))

(defmacro def-create-fn [entity & [parent] ]
  "Defines a function to build and save entity hashes."
  (let [args (compact [parent 'attributes])]
    `(defn ~(fn-name "create" entity) [~@args]
       (postprocess-entity (ds/create-entity (preprocess-entity (~(fn-name "make" entity) ~@args)))))))

(defmacro deffilter [entity name doc-string [property operator] & [result-fn]]
  "Defines a finder function for the entity."
	`(defn ~(symbol name) ~doc-string
  	[~property]
   	(~(or result-fn 'identity)
    ((filter-fn '~entity '~property ~operator) ~property))))

(defmacro def-find-all-by-property-fns [entity & properties]
  "Defines a function for each property, that find all entities by a property."
	`(do
  	~@(for [property properties]
    	 	`(do
        	(deffilter ~entity
          	~(symbol (find-entities-fn-name entity property))
            ~(find-entities-fn-doc entity property)
            (~property))))))

(defmacro def-find-first-by-property-fns [entity & properties]
  "Defines a function for each property, that finds the first entitiy by a property."
	`(do
  	~@(for [property properties]
    		`(do
					(deffilter ~entity
          	~(symbol (find-entity-fn-name entity property))
            ~(find-entity-fn-doc entity property)
            (~property) first)))))

(defmacro def-delete-fn [entity]
  "Defines a delete function for the entity."
	`(defn ~(symbol (str "delete-" entity)) [& ~'args]
  	(ds/delete-entity ~'args)))

(defmacro def-find-all-fn [entity]
  "Defines a function that returns all entities."
	`(defn ~(fn-name "find" (pluralize (str entity))) []
  	(map postprocess-entity (ds/find-all (Query. ~(str entity))))))

(defmacro def-update-fn [entity]
  "Defines an update function for the entity."
	`(defn ~(symbol (str "update-" entity)) [~entity ~'properties]
  		(ds/update-entity ~entity ~'properties)))

(defn serialize [object] (binding [*print-dup* true] (pr-str object)))
(defn deserialize [data] (with-in-str data (read)))

(defmacro reassoc [col & kv-seq]
	"Similar to assoc but only assigns the key and evaluates the value if the key already exists."
  (loop [kvs kv-seq
         result []]
    (if (empty? kvs)
      `((comp ~@result) ~col)
      (let [[k v & r] kvs]
        (recur r (conj result `(fn [col#] (if (contains? col# ~k) (assoc col# ~k ~v) col#))))))))

(defmacro def-preprocess-fn [entity & properties]
	"Defines a function to transform entity data before it is stored in the datastore."
	(let [complex-properties  (entity-properties properties :complex)
	 			serialized-source   (flatten-first-level (map #(serialize-property % entity) complex-properties))
				text-properties     (entity-properties properties :text)
				text-source   			(flatten-first-level (map #(property->text % entity) text-properties))]
		(if (and (empty? text-source) (empty? serialized-source)) 
			`(defmethod preprocess-entity ~(str entity) [~entity] ~entity)
 			`(defmethod preprocess-entity ~(str entity) [~entity] (reassoc ~entity ~@serialized-source ~@text-source)))))

(defmacro def-postprocess-fn [entity & properties]
	"Defines a function to transform entity data before it is stored in the datastore."
	(let [complex-properties  (entity-properties properties :complex)
	 			serialized-source   (flatten-first-level (map #(deserialize-property % entity) complex-properties))
				text-properties     (entity-properties properties :text)
				text-source   			(flatten-first-level (map #(text->property % entity) text-properties))]
		(if (and (empty? text-source) (empty? serialized-source)) 
			`(defmethod postprocess-entity ~(str entity) [~entity] ~entity)
			`(defmethod postprocess-entity ~(str entity) [~entity] (reassoc ~entity ~@serialized-source ~@text-source)))))

(defmacro defentity [entity parent & properties]
  "Defines helper functions for the entity. Note that
   if no property is qualified by :key true, then the data
   store will create a unique key for this object.  However
   note that the result of calling make-*entity*-key for any 
   such object is nil and not a proper key."
	`(do
		(def-preprocess-fn ~entity ~@properties)
		(def-postprocess-fn ~entity ~@properties)
    (def-key-fn ~entity ~(entity-properties properties :key) ~@parent)
    (def-make-fn ~entity ~parent ~@properties)
    (def-create-fn ~entity ~@parent)
    (def-delete-fn ~entity)
    (def-find-all-by-property-fns ~entity ~@(map first properties))
    (def-find-all-fn ~entity)
    (def-find-first-by-property-fns ~entity ~@(map first properties))
    (def-update-fn ~entity)))
