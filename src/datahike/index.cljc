(ns datahike.index
  (:require [datahike.index.hitchhiker-tree :as dih]
            [datahike.index.persistent-set :as dip])
  #?(:clj (:import [hitchhiker.tree DataNode IndexNode]
                   [me.tonsky.persistent_sorted_set PersistentSortedSet])))

;; TODO add doc to each function
(defprotocol IIndex
  (-all [index])
  (-seq [index])
  (-count [index])
  (-insert [index datom index-type])
  (-remove [index datom index-type])
  (-slice [index from to index-type])
  (-flush [index backend])
  (-transient [index])
  (-persistent! [index]))


(extend-type DataNode
  IIndex
  (-all [eavt-tree]
        (dih/-all eavt-tree :eavt))
  (-seq [eavt-tree]
        (dih/-seq eavt-tree :eavt))
  (-count [eavt-tree]
          (dih/-count eavt-tree :eavt))
  (-insert [tree datom index-type]
           (dih/-insert tree datom index-type))
  (-remove [tree datom index-type]
           (dih/-remove tree datom index-type))
  (-slice [tree from to index-type]
          (dih/-slice tree from to index-type))
  (-flush [tree backend]
          (dih/-flush tree backend))
  (-transient [tree]
              (dih/-transient tree))
  (-persistent! [tree]
               (dih/-persistent! tree)))


(extend-type IndexNode
  IIndex
  (-all [eavt-tree]
        (dih/-all eavt-tree :eavt))
  (-seq [eavt-tree]
        (dih/-seq eavt-tree :eavt))
  (-count [eavt-tree]
          (dih/-count eavt-tree :eavt))
  (-insert [tree datom index-type]
           (dih/-insert tree datom index-type))
  (-remove [tree datom index-type]
           (dih/-remove tree datom index-type))
  (-slice [tree from to index-type]
          (dih/-slice tree from to index-type))
  (-flush [tree backend]
          (dih/-flush tree backend))
  (-transient [tree]
              (dih/-transient tree))
  (-persistent! [tree]
               (dih/-persistent! tree)))


(extend-type PersistentSortedSet
  IIndex
  (-all [eavt-set]
        (dip/-all eavt-set))
  (-seq [eavt-set]
        (dip/-seq eavt-set))
  (-count [eavt-set]
          (dip/-count eavt-set))
  (-insert [set datom index-type]
           (dip/-insert set datom index-type))
  (-remove [set datom index-type]
           (dip/-remove set datom index-type))
  (-slice [set from to _]
          (dip/-slice set from to))
  (-flush [set _]
          (dip/-flush set))
  (-transient [set]
              (dip/-transient set))
  (-persistent! [set]
                (dip/-persistent! set)))



(defmulti empty-index
  "Creates empty index"
  {:arglists '([index index-type])}
  (fn [index index-type & opts] index))

(defmethod empty-index ::hitchhiker-tree
  [_ _ & {:keys [config]}]
  (dih/empty-tree config))

(defmethod empty-index ::persistent-set [_ index-type & _]
  (dip/empty-set index-type))


(defmulti init-index
  "Initialize index with datoms"
  {:arglists '([index datoms indexed index-type])}
  (fn [index datoms indexed index-type & opts] index))

(defmethod init-index ::hitchhiker-tree
  [_ datoms _ index-type & {:keys [config]}]
  (dih/init-tree datoms index-type config))

(defmethod init-index ::persistent-set [_ datoms indexed index-type & _]
  (dip/init-set datoms indexed index-type))
