(in-ns 'datahike.repl)

(require '[datahike.api :as d])

(defmethod print-method datahike.db.DB
  [this w]
  (.write ^java.io.Writer w "#datahike.db.DB{}"))

(require 'datahike-ddb-s3.core)
(defmethod print-method datahike_ddb_s3.core.DynamoDB+S3Store
  [this w]
  (.write ^java.io.Writer w
          (str "#datahike-ddb-s3.core.DynamoDB+S3Store{:table-name \""
               (:table-name this) "\" :bucket-name \"" (:bucket-name this) "\"}")))

(.setLevel (org.slf4j.LoggerFactory/getLogger ch.qos.logback.classic.Logger/ROOT_LOGGER_NAME) ch.qos.logback.classic.Level/INFO)
(.setLevel (org.slf4j.LoggerFactory/getLogger "datahike-ddb-s3.core") ch.qos.logback.classic.Level/INFO)

(require '[cognitect.aws.client.api :as aws])

(def ddb-client (aws/client {:api :dynamodb :region "us-west-2"}))
(def s3-client (aws/client {:api :s3 :region "us-west-2"}))

(d/create-database "datahike:ddb+s3://us-west-2/csm-datahike-test/csm-datahike-test")

(def conn (d/connect "datahike:ddb+s3://us-west-2/csm-datahike-test/csm-datahike-test"))

(def schema
  [{:db/ident :country/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value
    :db/doc "The name of the country"}

   ;; artist attributes
   {:db/ident :artist/gid
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/index true
    :db/doc "The globally unique MusicBrainz ID for an artist"}

   {:db/ident :artist/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "The artist's name"}

   {:db/ident :artist/sortName
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "The artist's name for use in alphabetical sorting, e.g. Beatles, The"}

   ; In latest version of db, this doc string is incorrectly listed as
   ; "The artist's name for use in sorting, e.g. Beatles, The"
   {:db/ident :artist/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "Enum, one of :artist.type/person, :artist.type/other, :artist.type/group."}

   {:db/ident :artist/gender
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "Enum, one of :artist.gender/male, :artist.gender/female, or :artist.gender/other."}

   {:db/ident :artist/country
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc "The artist's country of origin"}

   {:db/ident :artist/startYear
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "The year the artist started actively recording"}

   {:db/ident :artist/startMonth
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "The month the artist started actively recording"}

   {:db/ident :artist/startDay
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "The day the artist started actively recording"}

   {:db/ident :artist/endYear
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "The year the artist stopped actively recording"}

   {:db/ident :artist/endMonth
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "The month the artist stopped actively recording"}

   {:db/ident :artist/endDay
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "The day the artist stopped actively recording"}

   ;; abstractRelease attributes
   ;; NOTE: abstractRelease data are omitted from the
   ;; 1968-1973 mbrainz sample database
   {:db/ident :abstractRelease/gid
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/index true
    :db/doc "The globally unique MusicBrainz ID for the abstract release"}

   {:db/ident :abstractRelease/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "The name of the abstract release"}

   {:db/ident :abstractRelease/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "Enum, one
  of: :release.type/album, :release.type/single, :release.type/ep, :release.type/audiobook,
  or :release.type/other"}

   {:db/ident :abstractRelease/artists
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc "The set of artists contributing to the abstract release"}

   {:db/ident :abstractRelease/artistCredit
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "The string represenation of the artist(s) to be credited on the abstract release"}

   ;; release attributes
   {:db/ident :release/gid
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/index true
    :db/doc "The globally unique MusicBrainz ID for the release"}

   {:db/ident :release/country
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc "The country where the recording was released"}

   {:db/ident :release/barcode
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "The barcode on the release packaging"}

   {:db/ident :release/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "The name of the release"}

   {:db/ident :release/media
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/many
    :db/doc "The various media (CDs, vinyl records, cassette tapes, etc.) included in the release."}

   {:db/ident :release/packaging
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "The type of packaging used in the release, an enum, one
  of: :release.packaging/jewelCase, :release.packaging/slimJewelCase, :release.packaging/digipak, :release.packaging/other
  , :release.packaging/keepCase, :release.packaging/none,
  or :release.packaging/cardboardPaperSleeve"}

   {:db/ident :release/year
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "The year of the release"}

   {:db/ident :release/month
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "The month of the release"}

   {:db/ident :release/day
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "The day of the release"}

   {:db/ident :release/artistCredit
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "The string represenation of the artist(s) to be credited on the release"}

   {:db/ident :release/artists
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc "The set of artists contributing to the release"}

   {:db/ident :release/abstractRelease
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc "This release is the physical manifestation of the
  associated abstract release, e.g. the the 1984 US vinyl release of
  \"The Wall\" by Columbia, as opposed to the 2000 US CD release of
  \"The Wall\" by Capitol Records."}

   {:db/ident :release/status
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "The status of the release"}

   ;; media attributes
   {:db/ident :medium/tracks
    :db/valueType :db.type/ref
    :db/isComponent true
    :db/cardinality :db.cardinality/many
    :db/doc "The set of tracks found on this medium"}

   {:db/ident :medium/format
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "The format of the medium. An enum with lots of possible values"}

   {:db/ident :medium/position
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "The position of this medium in the release relative to the other media, i.e. disc 1"}

   {:db/ident :medium/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "The name of the medium itself, distinct from the name of the release"}

   {:db/ident :medium/trackCount
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "The total number of tracks on the medium"}

   ;; track attributes
   {:db/ident :track/artists
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc "The artists who contributed to the track"}

   {:db/ident :track/position
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "The position of the track relative to the other tracks on the medium"}

   {:db/ident :track/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "The track name"}

   {:db/ident :track/duration
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "The duration of the track in msecs"}

   #:db{:ident :release/script,
        :valueType :db.type/ref,
        :cardinality :db.cardinality/one,
        :doc "The script used in the release"}
   #:db{:ident :label/type,
        :valueType :db.type/keyword,
        :cardinality :db.cardinality/one,
        :doc "Enum, one of :label.type/distributor, :label.type/holding,
            :label.type/production, :label.type/originalProduction,
            :label.type/bootlegProduction, :label.type/reissueProduction, or
            :label.type/publisher."}
   #:db{:ident :label/endYear,
        :valueType :db.type/long,
        :cardinality :db.cardinality/one,
        :doc "The year the label stopped business"}
   #:db{:ident :label/startMonth,
        :valueType :db.type/long,
        :cardinality :db.cardinality/one,
        :doc "The month the label started business"}
   #:db{:ident :script/name,
        :valueType :db.type/string,
        :cardinality :db.cardinality/one,
        :unique :db.unique/value,
        :doc "Name of written character set, e.g. Hebrew, Latin, Cyrillic"}
   #:db{:ident :label/country,
        :valueType :db.type/ref,
        :cardinality :db.cardinality/one,
        :doc "The country where the record label is located"}
   #:db{:ident :release/labels,
        :valueType :db.type/ref,
        :cardinality :db.cardinality/many,
        :doc "The label on which the recording was released"}
   #:db{:ident :release/language,
        :valueType :db.type/ref,
        :cardinality :db.cardinality/one,
        :doc "The language used in the release"}
   #:db{:ident :label/sortName,
        :valueType :db.type/string,
        :cardinality :db.cardinality/one,
        :index true,
        :doc "The name of the record label for use in alphabetical sorting"}
   #:db{:ident :language/name,
        :valueType :db.type/string,
        :cardinality :db.cardinality/one,
        :unique :db.unique/value,
        :doc "The name of the written and spoken language"}
   #:db{:ident :label/gid,
        :valueType :db.type/uuid,
        :cardinality :db.cardinality/one,
        :unique :db.unique/identity,
        :doc "The globally unique MusicBrainz ID for the record label"}
   #:db{:ident :label/startDay,
        :valueType :db.type/long,
        :cardinality :db.cardinality/one,
        :doc "The day the label started business"}
   #:db{:ident :label/endMonth,
        :valueType :db.type/long,
        :cardinality :db.cardinality/one,
        :doc "The month the label stopped business"}
   #:db{:ident :label/endDay,
        :valueType :db.type/long,
        :cardinality :db.cardinality/one,
        :doc "The day the label stopped business"}
   #:db{:ident :label/startYear,
        :valueType :db.type/long,
        :cardinality :db.cardinality/one,
        :index true,
        :doc "The year the label started business"}
   #:db{:ident :label/name,
        :valueType :db.type/string,
        :cardinality :db.cardinality/one,
        :index true,
        :doc "The name of the record label"}
   #:db{:ident :track/artistCredit,
        :valueType :db.type/string,
        :cardinality :db.cardinality/one,
        :doc "The artists who contributed to the track"}])

(d/transact conn schema)

(def ref-types (set (map first (d/q '[:find ?a :in $ :where [?e :db/valueType :db.type/ref] [?e :db/ident ?a]] (d/db conn)))))

(def tempid-map (volatile! {}))

(defn ->txn
  [tx]
  (vec (map (fn [{:keys [e a v]}]
              [:db/add (str e) a
               (if (ref-types a)
                 (get @tempid-map (str v) (str v))
                 v)])
            tx)))

(defn transact*
  [t]
  (let [result (d/transact conn (->txn t))]
    (vswap! tempid-map into (:tempids result))))

(require '[clojure.java.io :as io])
(require '[clojure.edn :as edn])
(import '[java.util.zip GZIPInputStream])

(def mbrainz-dump (map edn/read-string (line-seq (io/reader (GZIPInputStream. (io/input-stream "../datahike-s3/mbrainz.edn.gz"))))))

(def mbrainz-txns (group-by :tx mbrainz-dump))
(def txns (sort (keys mbrainz-txns)))

(transact* (get mbrainz-txns (first txns)))
(transact* (get mbrainz-txns (second txns)))
(transact* (get mbrainz-txns (nth txns 2)))
(time (transact* (get mbrainz-txns (nth txns 3))))

(def current (atom 4))
(def running (atom true))

(require '[clojure.core.async :as async])
(async/thread
  (time
    (loop [items (drop 4 (map-indexed vector txns))]
      (when @running
        (when-let [[i t] (first items)]
          (reset! current i)
          (time (transact* (get mbrainz-txns t)))
          (recur (rest items)))))))
