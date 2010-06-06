(ns appengine.datastore.test.query
  (:import (com.google.appengine.api.datastore Query Query$FilterOperator Query$SortDirection))
  (:use clojure.test appengine.datastore.core appengine.datastore.query appengine.test))

(deftest test-filter-operator
  (testing "Valid filter operators"
    (are [operator expected]
      (is (= (filter-operator operator) expected))
      = Query$FilterOperator/EQUAL
      > Query$FilterOperator/GREATER_THAN
      >= Query$FilterOperator/GREATER_THAN_OR_EQUAL
      < Query$FilterOperator/LESS_THAN
      <= Query$FilterOperator/LESS_THAN_OR_EQUAL
      not Query$FilterOperator/NOT_EQUAL))
  (testing "Invalid filter operators"
    (is (thrown? IllegalArgumentException (filter-operator :invalid)))))

(deftest test-sort-direction
  (testing "Valid sort directions"
    (are [direction expected]
      (is (= (sort-direction direction) expected))
      :asc Query$SortDirection/ASCENDING
      :desc Query$SortDirection/DESCENDING))
  (testing "Invalid sort directions"
    (is (thrown? IllegalArgumentException (sort-direction :invalid)))))

(datastore-test test-query
  (testing "Build a new kind-less Query that finds Entity objects."
    (let [query (query)]
      (is (query? query))
      (is (nil? (.getKind query)))
      (is (= (str query) "SELECT *"))))
  (testing "Build a new Query that finds Entity objects with the
  specified Key as an ancestor."
    (let [query (query (create-key "continent" "eu"))]
      (is (query? query))
      (is (nil? (.getKind query)))
      (is (= (str query) "SELECT * WHERE __ancestor__ is continent(\"eu\")"))))
  (testing "Build a new Query that finds Entity objects with the
  specified kind."
    (let [query (query "continent")]
      (is (query? query))
      (is (= (.getKind query) "continent"))
      (is (= (str query) "SELECT * FROM continent"))))
  (testing "Build a new Query that finds Entity objects with the
  specified kind and with Key as an ancestor."
    (let [query (query "countries" (create-key "continent" "eu"))]
      (is (query? query))
      (is (= (.getKind query) "countries"))
      (is (= (str query) "SELECT * FROM countries WHERE __ancestor__ is continent(\"eu\")"))))
  (testing "Compound select queries"
    (are [q expected-sql]
      (do (is (query? q))
          (is (= (str q) expected-sql)))
      (-> (query "continents")
          (filter-by = :iso-3166-alpha-2 "eu")
          (filter-by > :country-count 0)
          (order-by :iso-3166-alpha-2 :desc))
      "SELECT * FROM continents WHERE iso-3166-alpha-2 = eu AND country-count > 0 ORDER BY iso-3166-alpha-2 DESC")))

(datastore-test test-filter-by
  (are [q expected-sql]
    (do (is (query? q))
        (is (= (str q) expected-sql)))
    (filter-by (query "continents") = :iso-3166-alpha-2 "eu")
    "SELECT * FROM continents WHERE iso-3166-alpha-2 = eu"
    (-> (query "continents")
        (filter-by = :iso-3166-alpha-2 "eu")
        (filter-by = :name "Europe"))
    "SELECT * FROM continents WHERE iso-3166-alpha-2 = eu AND name = Europe"))

(datastore-test test-order-by
  (are [q expected-sql]
    (do (is (query? q))
        (is (= (str q) expected-sql)))
    (order-by (query "continents") :iso-3166-alpha-2)
    "SELECT * FROM continents ORDER BY iso-3166-alpha-2"
    (order-by (query "continents") :iso-3166-alpha-2 :asc)
    "SELECT * FROM continents ORDER BY iso-3166-alpha-2"
    (order-by (query "continents") :iso-3166-alpha-2 :desc)
    "SELECT * FROM continents ORDER BY iso-3166-alpha-2 DESC"
    (-> (query "continents")
        (order-by :iso-3166-alpha-2)
        (order-by :name :desc))
    "SELECT * FROM continents ORDER BY iso-3166-alpha-2, name DESC"))

(datastore-test test-query?
  (are [arg expected] (is (= (query? arg) expected))
       (Query.) true
       nil false
       "" false))

(datastore-test test-select
  (let [q (select)]
    (is (query? q))
    (is (= (str q) "SELECT *")))
  (let [q (select "continents")]
    (is (query? q))
    (is (= (str q) "SELECT * FROM continents")))
  (let [q (select (create-key "continent" "eu"))]
    (is (query? q)))
  (let [q (select "countries" (create-key "continent" "eu"))]
    (is (query? q))
    (is (= (str q) "SELECT * FROM countries WHERE __ancestor__ is continent(\"eu\")")))
  (let [q (select "continents" where (= :name "Europe") order-by (:name))]
    (is (query? q))
    (is (= (str q) "SELECT * FROM continents WHERE name = Europe ORDER BY name")))
  (let [q (select "continents" order-by (:name) where (= :name "Europe"))]
    (is (query? q))
    (is (= (str q) "SELECT * FROM continents WHERE name = Europe ORDER BY name")))
  (let [q (select "continents" where (= :name "Europe") (> :updated-at "2010-01-01") order-by (:name) (:updated-at :desc))]
    (is (query? q))
    (is (= (str q) "SELECT * FROM continents WHERE name = Europe AND updated-at > 2010-01-01 ORDER BY name, updated-at DESC"))))
