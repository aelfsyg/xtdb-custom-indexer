(ns ae.ablution.db.dev
  (:require [ae.ablution.db.core :as db]
            [ae.ablution.db.indexer :as idx]
            [xtdb.api :as xt]))

(def alpha
  {:xt/id :alpha
   :a :b
   :c {:A :B :C {:ay :bee :cee "dee"}}
   :e "eff"
   :g ["aich" "haich"]})

(idx/doc->av alpha)
;; => ([:c|C|cee "dee"] [:e "eff"] [:g "aich"] [:g "haich"])

(def beta
  {:xt/id :beta
   :ae.ablution/address {:ae.ablution.address/first-line "The Barn"
                         :ae.ablution.address/second-line "The Street"}
   :ae.ablution/owner "Frank"})

(idx/doc->av beta)
;; => ([:ae.ablution/address|ae.ablution.address/first-line "The Barn"]
;;     [:ae.ablution/address|ae.ablution.address/second-line "The Street"]
;;     [:ae.ablution/owner "Frank"])

(->> [[::xt/put alpha] [::xt/put beta]]
     (xt/submit-tx db/node)
     (xt/await-tx db/node))

(db/find-by-field "eff")

(db/find-by-field "dee")

(db/find-by-field "Frank")

(db/find-by-field "Street")

(def gamma
  {:xt/id :gamma
   :freude "schoene"
   :gotterfunken {:tochter-als "Elysium"}})

(xt/await-tx db/node (xt/submit-tx db/node [[::xt/put gamma]]))

(db/find-by-field "schoene")

(db/find-by-field "Elysium")
