(ns ae.ablution.db.core
  (:require
   [xtdb.api :as xt]
   [xtdb.lucene :as lucene]
   [xtdb.codec :as codec]
   [ae.ablution.db.indexer :as idx]))

(def db-url "jdbc:postgresql://localhost:5432/ablution?user=ae")

(def xtdb-conf
  {:eav {:xtdb/module 'xtdb.lucene/->lucene-store
         :analyzer 'xtdb.lucene/->analyzer
         :indexer 'ae.ablution.db.indexer/->indexer}})

(defonce node (xt/start-node xtdb-conf))

(defn close [node]
  (.close node))

(defn q
  ([query] (xt/q (xt/db node) query))
  ([query in] (xt/q (xt/db node) query in)))

#_(defn entity [id]
    (xt/entity (xt/db node) id))

#_(defn find-entity
    ([term]
     (q '{:find [e v a s]
          :in [term]
          :where [[(wildcard-text-search term {:lucene-store-k :eav}) [[e v a s]]]]}
        term))
    ([term type]
     (q '{:find [e v a s]
          :in [[term type]]
          :where [[e :ae.ablution.entity/type type]
                  [(wildcard-text-search term {:lucene-store-k :eav}) [[e v a s]]]]}
        [term type])))

#_(defn put! [doc]
    (xt/submit-tx node [[::xt/put doc]]))

#_(defn del! [id]
    (xt/submit-tx node [[::xt/delete id]]))

#_(defn evict! [id]
    (xt/submit-tx node [[::xt/evict id]]))
