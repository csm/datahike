(in-ns 'datahike.repl)

(require '[datahike.api :as d])

(def conn (d/connect "datahike:ddb+s3://us-west-2/csm-datahike-test/csm-datahike-test/mbrainz"))

(require '[hitchhiker.tree :as hht])
(require '[clojure.core.async :as async])
(require '[hitchhiker.tree.node :as n])

(defn pull-addresses
  [tree]
  (if (hht/index-node? tree)
    (do
      (println :index-node (some-> (:storage-addr tree) (async/poll!)))
      (->> (:children tree)
           (reduce (fn [m child]
                     (println :child (type child))
                     (if-let [address (async/poll! (:storage-addr child))]
                       (assoc m address (pull-addresses
                                          (if (satisfies? n/IResolved child)
                                            child
                                            (try (n/-resolve-chan child)
                                                 (catch Exception e
                                                   {:address child
                                                    :exception e})))))
                       m))
                   {})))
    (do
      (println :data-node (some-> (:storage-addr tree) (async/poll!)))
      {})))

(require '[clojure.walk :as walk])

(defn all-addresses
  [address-tree]
  (let [addresses (volatile! (transient #{}))]
    (walk/prewalk (fn [e]
                    (if (map-entry? e)
                      (if (and (map? (val e)) (= [:exception :address] (keys (val e))))
                        (let [_ (println "resolving" (:konserve-key (:address (val e))) "again...")
                              resolved (pull-addresses (n/-resolve-chan (:address (val e))))]
                          (vswap! addresses conj! (key e))
                          [(key e) resolved])
                        (do (vswap! addresses conj! (key e))
                            e))
                      e))
                  address-tree)
    (persistent! @addresses)))

(def eavt {:eavt (pull-addresses (:eavt @conn))})
(def eavt-addresses (all-addresses eavt))

(def aevt {:aevt (pull-addresses (:aevt @conn))})
(def aevt-addresses (all-addresses aevt))

(def avet {:avet (pull-addresses (:avet @conn))})
(def avet-addresses (all-addresses avet))

(def temporal-eavt {:temporal-eavt (pull-addresses (:temporal-eavt @conn))})
(def temporal-eavt-addresses (all-addresses temporal-eavt))

(def temporal-aevt {:temporal-aevt (pull-addresses (:temporal-aevt @conn))})
(def temporal-aevt-addresses (all-addresses temporal-aevt))

(def temporal-avet {:temporal-avet (pull-addresses (:temporal-avet @conn))})
(def temporal-avet-addresses (all-addresses temporal-avet))

(let [exception-paths (volatile! [])
      path (volatile! [])]
  (walk/walk (fn [e]
               (when (map-entry? e)
                 (vswap! path conj (key e))
                 (when (instance? Exception (val e))
                   (vswap! exception-paths conj @path)))
               e)
             (fn [e]
               (when (map-entry? e)
                 (vswap! path pop))
               e)
             eavt)
  @exception-paths)

(require '[konserve-ddb-s3.core :refer [anomaly?]])

(def encode-key #'konserve-ddb-s3.core/encode-key)

(def index-addresses (set (map #(encode-key "mbrainz" %)
                               (concat eavt-addresses aevt-addresses avet-addresses temporal-eavt-addresses temporal-aevt-addresses temporal-avet-addresses))))

(def discarded (volatile! (transient #{})))
(def kept (volatile! (transient #{})))

(loop [continuation-token nil]
  (let [list (aws/invoke s3-client {:op :ListObjectsV2
                                    :request {:Bucket "csm-datahike-test"
                                              :ContinuationToken continuation-token}})]
    (when (anomaly? list)
      (throw (ex-info "error listing S3 objects" {:error list})))
    (printf "looking at %d objects...\n" (:KeyCount list))
    (doseq [object (:Contents list)]
      (if (index-addresses (:Key object))
        (vswap! kept conj! (:Key object))
        (vswap! discarded conj! (:Key object))))
    (when (:IsTruncated list)
      (recur (:NextContinuationToken list)))))

(doseq [discard (partition-all 1000 @discarded)]
  (println "deleting" (count discard) "keys...")
  (let [response (aws/invoke s3-client {:op :DeleteObjects
                                        :request {:Bucket "csm-datahike-test"
                                                  :Delete {:Objects (map #(hash-map :Key %) discard)}}})]
    (if (anomaly? response)
      (throw (ex-info "failed to delete S3 objects" {:error response})))))

(let [total-size (volatile! 0)
      count (volatile! 0)]
  (loop [continuation-token nil]
    (let [list (aws/invoke s3-client {:op :ListObjectsV2
                                      :request {:Bucket "csm-datahike-test"
                                                :ContinuationToken continuation-token}})]
      (when (anomaly? list)
        (throw (ex-info "failed to list objects" {:error list})))
      (doseq [object (:Contents list)]
        (vswap! total-size + (:Size object 0))
        (vswap! count inc))
      (when (:IsTruncated list)
        (recur (:NextContinuationToken list)))))
  {:total-size @total-size
   :count @count})