(ns appengine.datastore.query
  (:import (com.google.appengine.api.datastore Key Query Query$FilterOperator Query$SortDirection))
  (:use appengine.utils)
	(:use [clojure.contrib.seq :only (includes?)]))

(defn- ds-filter [[op prop value]]
  (let [ds-op-map {'=  com.google.appengine.api.datastore.Query$FilterOperator/EQUAL
                   '>  com.google.appengine.api.datastore.Query$FilterOperator/GREATER_THAN
                   '>= com.google.appengine.api.datastore.Query$FilterOperator/GREATER_THAN_OR_EQUAL
                   '<  com.google.appengine.api.datastore.Query$FilterOperator/LESS_THAN
                   '<= com.google.appengine.api.datastore.Query$FilterOperator/LESS_THAN_OR_EQUAL
                   '!= com.google.appengine.api.datastore.Query$FilterOperator/NOT_EQUAL
                   'in com.google.appengine.api.datastore.Query$FilterOperator/IN}
        ds-op     (op ds-op-map)
        ds-prop   (name prop)]
    `(.addFilter ~ds-op ~ds-prop ~value)))

(defn- ds-sort
  [arg]
  (let [ds-direction-map {'asc  com.google.appengine.api.datastore.Query$SortDirection/ASCENDING
                          'dec  com.google.appengine.api.datastore.Query$SortDirection/DESCENDING}]
    (if (seq? arg) 
      (let [ds-direction     ((second arg) ds-direction-map)
            ds-prop          (name (first arg))]
        `(.addSort ~ds-prop ~ds-direction))
      (let [ds-prop (name arg)] `(.addSort ~ds-prop)))))

(defn- map-implicit-list [col keys]
  (loop [result {}
         key    :no-key
         args   col]
    (if (empty? args)
      result
      (let [item (first args)]
        (if (includes? keys item)
          (recur result item (rest args))
          (let [key-col (key result)
                old-col (if (nil? key-col) [] key-col)]
            (recur (assoc result key (conj old-col item)) key (rest args))))))))

(defmacro select [ent & args]
  (let [query-obj `(Query. ~ent)
        arg-map (map-implicit-list args ['where 'sort])
        filters (map ds-filter ('where arg-map))
        sorts   (map ds-sort ('sort arg-map))]
    `(doto ~query-obj ~@filters ~@sorts)))
