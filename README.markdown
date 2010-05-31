# Clojure library for Google App Engine

This library is a Clojure API for [Google App
Engine](http://code.google.com/appengine). It is based on John Hume's
[appengine-clj](http://github.com/duelinmarkers/appengine-clj) and has
some enhancements. It is heavily refactored, so don't expect
compatibility with the original library.

### Documentation 

The [Autodoc](http://tomfaulhaber.github.com/autodoc) API documentation can be found
[here](http://r0man.github.com/appengine-clj).

### Install

[Leiningen](http://github.com/technomancy/leiningen) usage:
    [appengine "0.3-SNAPSHOT"]

### appengine.datastore.core

A convenience API for the
[com.google.appengine.api.datastore](http://code.google.com/appengine/docs/java/javadoc/index.html?com/google/appengine/api/datastore/package-tree.html)
package providing access to Google's schema-free datastore. It allows
you to work with immutable data structures instead of mutable Entity
instances.

Example:
    (create-entity {:kind "continent" :name "Europe"})
    => {:kind "continent" :key #<Key Continent(1138)> :name "Europe"}

### appengine.datastore.entities

A convenience API for the appengine.datastore namespace. Entity helper
functions can be generated with the <code>defentity</code> macro.

Examples:

    (defentity continent ()
      (iso-3166-alpha-2 :key true)
      (name))
     
    (defentity country (continent)
      (iso-3166-alpha-2 :key true)
      (iso-3166-alpha-3)
      (name))
     
    (defentity region (country)
      (code :key true)
      (name))
     
    (with-local-datastore
      (let [continent (create-continent {:name "Europe" :iso-3166-alpha-2 "eu"})
            country (create-country continent {:name "Spain" :iso-3166-alpha-2 "es"})
            region (create-region country {:name "Galicia" :code "SP58"})]
        (println continent)
        (println country)
        (println region)))
     
    ; this prints: 
     
    {:name Europe, :iso-3166-alpha-2 eu, :kind continent, :key #&lt;Key continent("eu")&gt;}
    {:name Spain, :iso-3166-alpha-2 es, :kind country, :key #&lt;Key continent("eu")/country("es")&gt;}
    {:code SP58, :name Galicia, :kind region, :key #&lt;Key continent("eu")/country("es")/region("SP58")&gt;}

Entities also support automatic preprocessing and postprocessing for
complex data types and strings over 500 characters. Long strings are
converted to datastore Text objects. Complex properties are 
serialized to strings and also stored as Text objects. In the below
example, authors is serialized and stored as a Text object and abstract
is stored as a text object. 

Example:

	(defentity citation ()
  	(authors :complex true)
  	(abstract :text true)
  	(journal)
  	(year)
  	(pages))

	(with-local-datastore 
		(let [cit (create-citation 
								 {:abstract "A very very long string."
								  :authors [{:last-name "Smith" :first-name "John"}
								            {:last-name "Wong" :first-name "Xu"}]
								  :journal "Journal of Generic Names"
								  :year 2010
								  :pages "100-200"})]
			(println cit )))
		
	; this prints:
	{:pages 100-200, :year 2010, :authors [{:last-name Smith, :first-name John} {:last-name Wong, :first-name Xu}], :abstract A very very long string., :journal Journal of Generic Names, :kind citation, :key #<Key citation(1)>}
		

### appengine.datastore.transactions

Transaction and retry support based on AppEngine semantics (see [DatastoreService low-level API for details](http://code.google.com/appengine/docs/java/javadoc/com/google/appengine/api/datastore/DatastoreService.html)).

<code>(with-transaction ...)</code> executes its body in a transaction.  In case of a DatastoreFailureException or a ConcurrentModificationException, the body's execution is retried *transaction-retries* times.  Beware that if the retry-count is reached and an exception is thrown within the body of the transaction, the transaction is thrown out of the <code>(with-transaction...)</code>.

The transaction functionality works with both appengine.datastore.core and appengine.datastore.entities.

Examples:
    ;; Either creates both entities or neither if too many datastore exceptions.
    ;; Returns the second entity, as expected.
    (with-transaction
      (let [parent (ds/create-entity {:kind "Person" :name "jane"})]
        (ds/create-entity {:kind "Person" :name "bob" 
                     :parent-key (:key parent)})))
     
    ;; You can set the number of retries to deviate from the default (4)
    (with-retries 2 (with-transaction ... ))

Transactions automatically rollback as per the low-level API specs.  Additionally, appengine-clj supports manual rollback using <code>(rollback-transaction)</code>.  Within a <code>(with-transaction ...)</code>, you may check whether the current transaction is active through <code>(is-transaction-active?)</code>.  These can be used together to get consistent snapshots of parts of the datastore.

    (with-transaction
      ...
      (if (and something-went-wrong (is-transaction-active?))
        (rollback-transaction)))

You can nest transactions when working with two entity groups, but each transaction's success is independent of the other.

    (with-transaction ;; group1
      (let [parent-group1 (ds/create-entity {:kind "Person" :name "jane"})
            child-group1 (ds/create-entity {:kind "Child" :name "tamara"
                   		       	:parent-key (:key parent-group1)})]
        (with-transaction ;; nested group2
          (let [parent-group2 (ds/create-entity {:kind "Person" :name "berni"})
                child-group2 (ds/create-entity {:kind "Child" :name "eric"
                   		       	:parent-key (:key parent-group2)})]
            ...
      ))))

You can execute datastore operations outside of the current transaction through <code>(without-transaction)</code>.  Note: <code>(without-transaction)</code> has no retry semantics and should typically be surrounded by a (try ... (catch ...)) so that errors do not affect the surrounding transaction.

    (with-transaction
      ...
      (try {
        (without-transaction 
          (ds/create-entity {:kind "Person" :name "andy"}))
        (catch ...))
      ...
    )

### appengine.memcache

A convenience API for the [com.google.appengine.api.memcache](http://code.google.com/intl/ja/appengine/docs/java/javadoc/index.html?com/google/appengine/api/memcache/package-tree.html) package providing access to Google's memcache.

Example:
    (put-value "name" "Monkey D. Luffy") ;; save value
    (get-value "name") ;;=> "Monkey D. Luffy"
    (delete-value "name")
    
    ;; with Expiretaion
    (put-value "bounty" 100000000 3600) ;; expire after 1h
    (put-value "bounty" 100000000 (Date. 2010 10 3)) ;; specify expire date
    
    ;; with Policy
    (add-value "ability" "GomGom") ;; add only if not present
    (replace-value "bounty" 300000000) ;; replace only if present
    (set-value "sex" "male") ;; set always (same as put-value)
    
    ;; inc/dec value
    (put-value "member-count" 8)
    (inc-value "member-count") ;;=> 9
    (dec-value "member-count") ;;=> 8
    (inc-value "member-count" 10) ;;=> 18

### appengine.urlfetch

A convenience API for the [com.google.appengine.api.urlfetch](http://code.google.com/intl/ja/appengine/docs/java/javadoc/index.html?com/google/appengine/api/urlfetch/package-tree.html) package.

Example:
    (def *url* "http://www.google.com/")
    (fetch *url*)
    
    ; This returns response as map.
    {:status-code 200,
     :headers {:Content-Type "text/html; charset=utf-8",...}
     :content "<html>..."}
     
    ; fetch with options
    (fetch *url* {:method "POST", :follow-redirects false})
    
Supported Options:

* method (<code>"GET"</code> by default)
* payload
* headers
* allow-truncate
* follow-redirects (<code>true</code> by default)

### appengine.users

Convenience API for the
[com.google.appengine.api.users](http://code.google.com/appengine/docs/java/javadoc/index.html?com/google/appengine/api/datastore/package-tree.html)
package.

The <code>user-info</code> function can be called from anywhere to get
the current user and a UserService.  For a more functional approach, a
Ring middlware function is provided to assoc the user info into every
request under the key :appengine-clj/user-info.

### appengine.test

Provides setup and teardown of a local service for use in tests or
from a REPL. If you're using <code>clojure.test</code>, you can use
the usefull macros(<code>datastore-test</code>,
<code>memcache-test</code>, <code>urlfetch-test</code>) to get a fresh
environment for each test.

---

Copyright (c) 2009, 2010 John D. Hume, Roman Scherer, Jean-Denis
Greze, 深町英太郎 (E.Fukamachi).

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
