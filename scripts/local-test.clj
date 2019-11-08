(in-ns 'datahike.repl)

(import '[com.amazonaws.services.dynamodbv2.local.server DynamoDBProxyServer LocalDynamoDBServerHandler LocalDynamoDBRequestHandler])

(defn start-dynamodb-local
  []
  (let [ddb-handler (LocalDynamoDBRequestHandler. 0 true nil false)
        server-handler (LocalDynamoDBServerHandler. ddb-handler nil)
        server (doto (DynamoDBProxyServer. 0 server-handler) (.start))
        server-field (doto (.getDeclaredField DynamoDBProxyServer "server")
                       (.setAccessible true))
        jetty-server (.get server-field server)
        port (-> jetty-server (.getConnectors) first (.getLocalPort))]
    {:server server
     :port port}))

(require 's4.core)

(def s4 (s4.core/make-server! {}))
(def ddb (start-dynamodb-local))

(def access-key "ACCESS")
(def secret-key "SECRET")
(swap! (-> @s4 :auth-store :access-keys) assoc access-key secret-key)

(require '[datahike.api :as d])
(require '[cognitect.aws.credentials :as creds])
(require '[cognitect.aws.client.api :as aws])

(def local-ddb-client (aws/client {:api :dynamodb
                                   :credentials-provider (creds/basic-credentials-provider {:access-key-id access-key
                                                                                            :secret-access-key secret-key})
                                   :endpoint-override {:protocol "http"
                                                       :hostname "localhost"
                                                       :port (:port ddb)}}))
(def local-s3-client (aws/client {:api :s3
                                  :credentials-provider (creds/basic-credentials-provider {:access-key-id access-key
                                                                                           :secret-access-key secret-key})
                                  :endpoint-override {:protocol "http"
                                                      :hostname "localhost"
                                                      :port (.getPort (:bind-address @s4))}}))

(require 'datahike-ddb-s3.core)

(binding [datahike.store/*ddb-client* local-ddb-client
          datahike.store/*s3-client* local-s3-client]
  (d/create-database "datahike:ddb+s3://us-west-2/test/test"))

(binding [datahike.store/*ddb-client* local-ddb-client
          datahike.store/*s3-client* local-s3-client]
  (def conn (d/connect "datahike:ddb+s3://us-west-2/test/test")))

(d/transact conn {:tx-data [{:db/ident :test/name
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one
                             :db/unique :db.unique/identity}
                            {:db/ident :test/value
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one}]})

(d/transact conn {:tx-data [{:db/id "foo"
                             :test/name "foo"
                             :test/value "bar"}]})

(d/q '[:find ?n ?v :in $ :where [?e :test/name ?n] [?e :test/value ?v]] (d/db conn))

(.setLevel (org.slf4j.LoggerFactory/getLogger "datahike-ddb-s3.core") ch.qos.logback.classic.Level/INFO)
(dotimes [i 600]
  (prn (d/transact conn {:tx-data [{:db/id "x"
                                    :test/name (str "test" i)
                                    :test/value (str "test" i)}]})))