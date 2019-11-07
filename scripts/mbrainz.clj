(in-ns 'datahike.repl)

(require '[datahike.api :as d])

(defmethod print-method datahike.db.DB
  [this w]
  (.write ^java.io.Writer w "#datahike.db.DB{}"))