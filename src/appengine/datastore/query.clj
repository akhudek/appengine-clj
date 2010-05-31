(ns appengine.datastore.query
  (:import (com.google.appengine.api.datastore Key Query Query$FilterOperator Query$SortDirection))
  (:refer-clojure :exclude [sort-by])
  (:use appengine.utils))

(defn filter-operator
  "Returns the FilterOperator enum for the given operator. The
operator argument is a clojure function, such as =, >, >=, <, <= or
not. These functions are not called at all, they just act as shortcut
to map to the FilterOperator enums.

Examples:

  (filter-operator =)
  ; => #<FilterOperator =>

  (filter-operator >)
  ; => #<FilterOperator >>

  (filter-operator not)
  ; => #<FilterOperator !=>
"
  [operator]
  (cond
   (= operator =) Query$FilterOperator/EQUAL
   (= operator >) Query$FilterOperator/GREATER_THAN
   (= operator >=) Query$FilterOperator/GREATER_THAN_OR_EQUAL
   (= operator <) Query$FilterOperator/LESS_THAN
   (= operator <=) Query$FilterOperator/LESS_THAN_OR_EQUAL
   (= operator not) Query$FilterOperator/NOT_EQUAL
   :else (throw (IllegalArgumentException. (str "Invalid filter operator: " operator)))))

(defn sort-direction
  "Returns the SortDirection enum for the given sort direction. The
direction argument must be :asc for an ascending, or :desc for a
descending a sort order.

Examples:

  (sort-direction :asc)
  ; => #<SortDirection ASCENDING>

  (sort-direction :desc)
  ; => #<SortDirection DESCENDING>
"  
  [direction]
  (cond
   (= direction :asc) Query$SortDirection/ASCENDING
   (= direction :desc) Query$SortDirection/DESCENDING
   :else (throw (IllegalArgumentException. (str "Invalid sort direction: " direction)))))

(defmulti query
  "Create a new Query that finds Entity objects.

Examples:

  (query)
  ; => #<Query SELECT *>

  (query \"continents\")
  ; => #<Query SELECT * FROM continents>

  (query (create-key \"continent\" \"eu\"))
  ; => #<Query SELECT * WHERE __ancestor__ is continent(\"eu\")>

  (query \"countries\" (create-key \"continent\" \"eu\"))
  ; => #<Query SELECT * FROM countries WHERE __ancestor__ is continent(\"eu\")>
"
  (fn [& args] (map class args)))

(defmethod query [] []
  (Query.))

(defmethod query [Key] [key]
  (Query. key))

(defmethod query [String] [kind]
  (Query. kind))

(defmethod query [String Key] [kind key]
  (Query. kind key))

(defn filter-by
  "Add a filter on the specified property to the query.

Examples:

  (filter-by (query \"continents\") :iso-3166-alpha-2 = \"eu\")
  ; => #<Query SELECT * FROM continents WHERE iso-3166-alpha-2 = eu>

  (-> (query \"continents\")
      (filter-by :iso-3166-alpha-2 = \"eu\")
      (filter-by :name = \"Europe\"))
  ; => #<Query SELECT * FROM continents WHERE iso-3166-alpha-2 = eu AND name = Europe>
"
  [query property-name operator value]
  (.addFilter query (stringify property-name) (filter-operator operator) value))

(defn sort-by
  "Specify how the query results should be sorted. The first call to
sort-by will register the property that will serve as the primary
sort key. A second call to sort-by will set a secondary sort key,
etc. If no direction is given, the query results will be sorted in
ascending order of the given property.

Examples:

  (sort-by (query \"continents\") :iso-3166-alpha-2)
  ; => #<Query SELECT * FROM continents ORDER BY iso-3166-alpha-2>

  (-> (query \"continents\")
      (sort-by :iso-3166-alpha-2)
      (sort-by :name :desc))
  ; => #<Query SELECT * FROM continents ORDER BY iso-3166-alpha-2, name DESC>
"
  [query property-name & [direction]]
  (.addSort query (stringify property-name)
            (if direction (sort-direction direction) Query$SortDirection/ASCENDING)))

(defn query?
  "Returns true, if the arg is an instance of Query."
  [arg] (isa? (class arg) Query))

(defmacro select
  "A macro that transforms the select clause, and any number of
filter-by and sort-by clauses into a -> form to produce a query.

Examples:

  (select \"continents\"
    (filter-by :iso-3166-alpha-2 = \"eu\")
    (sort-by :iso-3166-alpha-2)
    (sort-by :name :desc))
  ; => #<Query SELECT * FROM continents WHERE iso-3166-alpha-2 = eu ORDER BY iso-3166-alpha-2, name DESC>

"
  [query & body]
  `(-> (query ~query)
       ~@body))


