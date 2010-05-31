(ns #^{:author "Roman Scherer"
       :doc "The core API for the Google App Engine datastore service." }
  appengine.datastore.core
  (:import (com.google.appengine.api.datastore
            DatastoreServiceFactory DatastoreServiceConfig DatastoreServiceConfig$Builder
            Entity Key Query KeyFactory Transaction))
  (:use appengine.utils [clojure.contrib.def :only (defvar)]))

(defvar *transaction* nil
  "The current datastore transaction. If set to nil, all operations
  are done without a transaction (the default behaviour).")

(defn datastore
  "Creates a DatastoreService using the default or the provided
configuration.

Examples:

  (datastore)
  ; => #<DatastoreServiceImpl com.google.appengine.api.datastore.DatastoreServiceImpl@a7b68a>"
  ([] (datastore (DatastoreServiceConfig$Builder/withDefaults)))
  ([#^DatastoreServiceConfig configuration] (DatastoreServiceFactory/getDatastoreService configuration)))

(defn create-key
  "Creates a new Key using the given kind and identifier. If parent-key is
given, the new key will be a child of the parent-key.
  
Examples:

   (create-key \"country\" \"de\")
   ; => #<Key country(\"de\")>
 	
   (create-key (create-key \"continent\" \"eu\") \"country\" \"de\")
   ; => #<Key continent(\"eu\")/country(\"de\")>"
  ([kind identifier]
     (create-key nil kind identifier))
  ([#^Key parent-key kind identifier]
     (KeyFactory/createKey
      parent-key kind 
      (if (integer? identifier) (Long/valueOf (str identifier)) 
          (str identifier)))))

(defn key->string
  "Returns a \"websafe\" string from the given Key.

Examples:

  (key->string (create-key \"continent\" \"eu\"))
  ; => \"agR0ZXN0chELEgljb250aW5lbnQiAmV1DA\"

   (key->string (create-key (create-key \"continent\" \"eu\") \"country\" \"de\")
   ; => \"agR0ZXN0ciALEgljb250aW5lbnQiAmV1DAsSB2NvdW50cnkiAmRlDA\""
  [key] (KeyFactory/keyToString key))

(defn string->key
  "Returns a Key from the given \"websafe\" string.

Examples:

  (string->key \"agR0ZXN0chELEgljb250aW5lbnQiAmV1DA\")
  ; => #<Key country(\"de\")>

   (string->key \"agR0ZXN0ciALEgljb250aW5lbnQiAmV1DAsSB2NvdW50cnkiAmRlDA\")
   ; => #<Key continent(\"eu\")/country(\"de\")>"
  [string] (KeyFactory/stringToKey string))

(defn entity->map
  "Converts a com.google.appengine.api.datastore.Entity instance to a
PersistentHashMap. The properties of the entity are stored under their
keyword names, the entity kind under :kind and the entity key
under :key.

Examples:

   (entity->map (doto (Entity. \"continent\") (.setProperty \"name\" \"Europe\")))
   ; => {:name \"Europe\", :kind \"continent\", :key #<Key continent(no-id-yet)>}"
  [#^Entity entity]
  (reduce #(assoc %1 (keyword (key %2)) (val %2))
	  (merge {:kind (.getKind entity) :key (.getKey entity)})
	  (.entrySet (.getProperties entity))))

(declare properties)

(defn map->entity
  "Converts a map or struct to an Entity. The map must have the key or
kind of the entity stored under the :key or a :kind keywords.

Examples:

   (map->entity {:key \"continent\" :name \"Europe\"})
   ; => #<Entity <Entity [continent(no-id-yet)]:
   ;      name = Europe"
  [map]
  (let [map (map-keyword map)]
   (reduce #(do (.setProperty %1 (name (first %2)) (second %2)) %1)
           (if (and (:kind map) (:parent-key map))
             (Entity. (:kind map) (:parent-key map))
             (Entity. (or (:key map) (:kind map))))
           (properties map))))

(defmulti properties
  "Returns the properties of the given record as a map."
  class)

(defmethod properties Entity [#^Entity entity]
  (properties (entity->map entity)))

(defmethod properties :default [map]
  (dissoc (merge {} map) :key :kind))

(defn filter-keys [keys]
  "Takes a sequences and returns a sequence of Key objects, doing
   conversions from Entity or map (with a :key), if possible."
  (remove nil? (map #(cond (instance? Key %) %
			   (instance? Entity %) (.getKey %)
			   (instance? Key (:key %)) (:key %)) keys)))

(defmulti get-entity
  "Retrieves a PersistentHashMap of an Entity in the datastore. The
  entity can be retrieved by an instance of Key, Entity, a
  PersistentHashMap with a :key keyword, or an ISeq of keys, in which 
  case a PersistentHashMap of appengine Keys to PersistentHashmaps of
  Entities is returned."
  class)

(defmethod get-entity Entity [#^Entity entity]
  (get-entity (.getKey entity)))

(defmethod get-entity Key [#^Key key]
  (entity->map (.get (datastore) *transaction* key)))

(defmethod get-entity clojure.lang.PersistentVector [keys]
  (get-entity (seq keys)))

(defmethod get-entity clojure.lang.ISeq [keys]
  (into {} (for [[key entity] (.get (datastore) *transaction* (filter-keys keys))] 
	     [key (entity->map entity)])))

(defmethod get-entity :default [map]
  (if-let [key (:key map)]
    (entity->map (.get (datastore) *transaction* key))))

(defmulti put-entity
  "Puts the given record into the datastore and returns a
  PersistentHashMap of the record. The record must be an instance of
  Entity or PersistentHashMap."
  class)

(defmethod put-entity Entity [#^Entity entity]
  (let [key (.put (datastore) *transaction* entity)]
    (assoc (entity->map entity) :key key)))

(defmethod put-entity :default [map]
  (put-entity (map->entity map)))

(defn find-all
  "Executes the given com.google.appengine.api.datastore.Query
  and returns the results as a lazy sequence of items converted 
  with entity->map."
  [#^Query query]
  (let [data-service (datastore)
        results (.asIterable (.prepare data-service *transaction* query))]
    (map entity->map results)))

(defn create-entity 
  "Takes a map of keyword-value pairs or struct and creates a new Entity
in the Datastore.  The map or struct must include a :kind
String. Returns the saved Entity converted with entity->map (which
will include the assigned :key)."
  [record]
  (put-entity record))

(defmulti update-entity
  "Updates the record with the given properites. The record must be an
instance of Entity, Key or PersistentHashMap. If one of the
attributes' values is :remove, then the correponding property name is
removed from that entity.  If one of the attributes't values is nil,
then the corresponding property name is set to Java's null for that
property."
  (fn [record attributes] (class record)))

(defmethod update-entity Entity [#^Entity entity attributes]
  (doseq [[attribute value] attributes]
    (if (not= value :remove)
      (.setProperty entity (name attribute) value)
      (.removeProperty entity (name attribute))))
  (put-entity entity))

(defmethod update-entity Key [#^Key key attributes]
  (update-entity (get-entity key) attributes))

(defmethod update-entity :default [map attributes]
  (update-entity (map->entity map) attributes))

(defmulti delete-entity class)

(defmethod delete-entity Entity [#^Entity entity]
  (delete-entity (.getKey entity)))

(defmethod delete-entity Key [key]
  (.delete (datastore) *transaction* (into-array [key])))

(defmethod delete-entity clojure.lang.PersistentVector [keys]
  (delete-entity (seq keys)))

(defmethod delete-entity clojure.lang.ISeq [keys]
  (.delete (datastore) *transaction* (filter-keys keys)))

(defmethod delete-entity :default [map]
  (if-let [key (:key map)]
    (delete-entity key)))



