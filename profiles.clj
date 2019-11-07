{:repl {:repl-options {:init-ns datahike.repl}
        :source-paths ["scripts"]
        :resource-paths ["repl-resources"]
        :dependencies [[s4 "0.1.9"]
                       [com.amazonaws/DynamoDBLocal "1.11.477"
                        :exclusions [com.fasterxml.jackson.core/jackson-core
                                     org.eclipse.jetty/jetty-client
                                     com.google.guava/guava
                                     commons-logging]]
                       [org.eclipse.jetty/jetty-server "9.4.15.v20190215"]
                       [com.almworks.sqlite4java/libsqlite4java-osx "1.0.392" :extension "dylib"]
                       [com.almworks.sqlite4java/libsqlite4java-linux-amd64 "1.0.392" :extension "so"]
                       [manifold "0.1.6"]
                       [ch.qos.logback/logback-core "1.2.3"]
                       [ch.qos.logback/logback-classic "1.2.3"]]
        :repositories [["aws-dynamodb-local" {:url "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"}]]
        :jvm-opts ["-Dsqlite4java.library.path=/Users/cmarshall/.m2/repository/com/almworks/sqlite4java/libsqlite4java-osx/1.0.392/"]}}