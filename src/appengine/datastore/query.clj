(ns appengine.datastore.query
  (:import (com.google.appengine.api.datastore Key Query Query$FilterOperator Query$SortDirection))
  (:use appengine.utils [clojure.contrib.seq :only (includes?)]))

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

  (filter-by (query \"continents\") = :iso-3166-alpha-2 \"eu\")
  ; => #<Query SELECT * FROM continents WHERE iso-3166-alpha-2 = eu>

  (-> (query \"continents\")
      (filter-by = :iso-3166-alpha-2 \"eu\")
      (filter-by = :name \"Europe\"))
  ; => #<Query SELECT * FROM continents WHERE iso-3166-alpha-2 = eu AND name = Europe>
"
  [query operator property-name value]
  (.addFilter query (stringify property-name) (filter-operator operator) value))

(defn order-by
  "Specify how the query results should be sorted. The first call to
order-by will register the property that will serve as the primary
sort key. A second call to order-by will set a secondary sort key,
etc. If no direction is given, the query results will be sorted in
ascending order of the given property.

Examples:

  (order-by (query \"continents\") :iso-3166-alpha-2)
  ; => #<Query SELECT * FROM continents ORDER BY iso-3166-alpha-2>

  (-> (query \"continents\")
      (order-by :iso-3166-alpha-2)
      (order-by :name :desc))
  ; => #<Query SELECT * FROM continents ORDER BY iso-3166-alpha-2, name DESC>
"
  [query property-name & [direction]]
  (.addSort query (stringify property-name)
            (if direction (sort-direction direction) Query$SortDirection/ASCENDING)))

(defn query?
  "Returns true, if the arg is an instance of Query."
  [arg] (isa? (class arg) Query))

(defn- extract-clauses
  "Extract and return the query, where and order-by clauses."
  [& args]
  (let [[query k1 v1 k2 v2] (partition-by #(includes? ['where 'order-by] %1) (first args))]
    (if (= (first k1) 'where) [query v1 v2] [query v2 v1])))

(defmacro select
  "A macro that transforms the select clause, and any number of
where and order-by clauses into a -> form to produce a query.

Examples:

  (select \"continents\")
  ; => #<Query SELECT * FROM continents>

  (select \"countries\" (create-key \"continent\" \"eu\") where (= :name \"Germany\"))
  ; => #<Query SELECT * FROM countries WHERE name = Germany AND __ancestor__ is continent(\"eu\")>

  (select \"continents\"
    where (= :name \"Europe\") (> :updated-at \"2010-01-01\")
    order-by (:name) (:updated-at :desc)))
  ; => #<Query SELECT * FROM continents WHERE name = Europe AND updated-at > 2010-01-01 ORDER BY name, updated-at DESC>
"
  [& args]
  (let [[query-clauses filter-clauses sort-clauses] (extract-clauses args)]
    `(-> (query ~@query-clauses)
         ~@(map (fn [args] `(filter-by ~@args)) filter-clauses)
         ~@(map (fn [args] `(order-by ~@args)) sort-clauses))))
