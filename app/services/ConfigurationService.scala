package services

import com.google.inject.Inject
import play.api.Configuration

class ConfigurationService @Inject()(conf: Configuration) {
  val mongoPrimaryHost: String = conf.underlying.getString("mongosync.mongo.in-sync")
  val mongoSecondaryHost: String = conf.underlying.getString("mongosync.mongo.out-of-sync")
  val mongoOplogSize: Int = conf.underlying.getInt("mongosync.mongo.oplog-size-GB")

  val internalDatabase: String = conf.underlying.getString("mongosync.internal.database")
  val testWriteCollection: String = conf.underlying.getString("mongosync.test-write.collection")
  val testWriteSize: Int = conf.underlying.getInt("mongosync.test-write.size-GB")
  val testWriteDocumentSize: Int = conf.underlying.getInt("mongosync.test-write.document-bytes")
  val maximumSeeds: Int = conf.underlying.getInt("mongosync.clone.maximum-seeds")
  val maximumThreads: Int = conf.underlying.getInt("mongosync.clone.maximum-threads")
}
