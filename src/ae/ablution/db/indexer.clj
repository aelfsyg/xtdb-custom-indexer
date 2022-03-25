(ns ae.ablution.db.indexer
  (:require
   [xtdb.api :as xt]
   [xtdb.lucene :as lucene]
   [xtdb.codec :as codec])
  (:import
   org.apache.lucene.analysis.Analyzer
   [org.apache.lucene.document Document Field$Store StringField TextField]
   [org.apache.lucene.index IndexWriter Term]
   [org.apache.lucene.search Query TermQuery]
   org.apache.lucene.analysis.custom.CustomAnalyzer
   [org.apache.lucene.analysis.core LowerCaseFilterFactory]
   [org.apache.lucene.analysis.standard StandardTokenizerFactory]
   [org.apache.lucene.analysis.ngram NGramTokenizerFactory]))

#_{:clj-kondo/ignore [:unused-namespace]}
(require
 '[ae.ablution :as-alias ablu]
 '[ae.ablution.address :as-alias address]
 '[ae.ablution.address.county :as-alias county]
 '[ae.ablution.agent :as-alias ablu.agent]
 '[ae.ablution.confirm :as-alias confirm]
 '[ae.ablution.customer :as-alias customer]
 '[ae.ablution.entity :as-alias entity]
 '[ae.ablution.entity.id :as-alias entity.id]
 '[ae.ablution.entity.type :as-alias entity.type]
 '[ae.ablution.employee :as-alias employee]
 '[ae.ablution.laundry :as-alias laundry]
 '[ae.ablution.laundry.batch :as-alias batch]
 '[ae.ablution.laundry.pile :as-alias pile]
 '[ae.ablution.person :as-alias person]
 '[ae.ablution.person.contact :as-alias contact]
 '[ae.ablution.person.title :as-alias person.title]
 '[ae.ablution.property :as-alias property]
 '[ae.ablution.vehicle :as-alias vehicle])

(def parent-attrs
  #{:c :C
    :ae.ablution/address
    :gotterfunken})

(defn combine [k1 k2]
  (if (some? k1)
    (keyword (str (subs (str k1) 1)
                  "|" (subs (str k2) 1)))
    k2))

(defn raise [p [a v]]
  (let [k (combine p a)]
    (if (parent-attrs a)
      (mapcat #(raise k %) v)
      [[k v]])))

(defn doc->av
  "Converts a document into a list of all the attibute-value pairings within it."
  [doc]
  (->> (dissoc doc :crux.db/id)
       (mapcat #(raise nil %))
       (mapcat (fn [[a v]]
                 (for [v (codec/vectorize-value v)
                       :when (string? v)]
                   [a v])))))

(defn- orig-doc->ac
  "This is only here for reference."
  [doc]
  (->> (dissoc doc :crux.db/id)
       (mapcat (fn [[a v]]
                 (for [v (codec/vectorize-value v)
                       :when (string? v)]
                   [a v])))))

(defrecord AbluIndexer []
  lucene/LuceneIndexer

  (index! [_ index-writer docs]
    (doseq [{e :crux.db/id, :as doc} (vals docs)
            [a v] (doc->av doc)
            :let
            [id-str (lucene/->hash-str (lucene/->DocumentId e a v))
             doc (doto (Document.)
                   ;; To search for triples by e-a-v for deduping
                   (.add (StringField. lucene/field-xt-id, id-str, Field$Store/NO))
                   ;; The actual term, which will be tokenized
                   (.add (TextField. (lucene/keyword->k a), v, Field$Store/YES))
                   ;; Used for wildcard searches
                   (.add (TextField. lucene/field-xt-val, v, Field$Store/YES))
                   ;; Used for eviction
                   (.add (StringField. lucene/field-xt-eid, (lucene/->hash-str e), Field$Store/NO))
                   ;; Used for wildcard searches
                   (.add (StringField. lucene/field-xt-attr, (lucene/keyword->k a), Field$Store/YES)))]]
      (.updateDocument ^IndexWriter index-writer (Term. lucene/field-xt-id id-str) doc)))

  (evict! [_ index-writer eids]
    (let [qs (for [eid eids]
               (TermQuery. (Term. lucene/field-xt-eid (lucene/->hash-str eid))))]
      (.deleteDocuments ^IndexWriter index-writer
                        ^"[Lorg.apache.lucene.search.Query;" (into-array Query qs)))))

(defn ->indexer [_]
  (AbluIndexer.))

#_(defn ^Analyzer ->analyzer [_]
    (.build (doto (CustomAnalyzer/builder)
              (.withTokenizer ^String NGramTokenizerFactory/NAME
                              ^"[Ljava.lang.String;" (into-array String ["minGramSize" "1" "maxGramSize" "7"]))
              (.addTokenFilter ^String LowerCaseFilterFactory/NAME
                               ^"[Ljava.lang.String;" (into-array String [])))))
