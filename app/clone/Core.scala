package clone

import java.util.concurrent.Executors

import clone.structure.{Collection, CollectionPart}
import com.google.inject.Inject
import org.slf4j.LoggerFactory
import services.{ConfigurationService, MongoService}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Handle the entire cloning of the instance
  */
class Core @Inject()(conf: ConfigurationService){
  val logger = LoggerFactory.getLogger("mongosync.clone.Core")
  val primary = new MongoService(conf, conf.mongoPrimaryHost)
  val secondary = new MongoService(conf, conf.mongoSecondaryHost)
  val pool = Executors.newFixedThreadPool(conf.maximumThreads)
  implicit val ec =  ExecutionContext.fromExecutor(pool)

  def sync(): Future[List[Unit]] = {
    val res: List[Future[Unit]] = generateCollections().flatMap(collection => {
      logger.debug(s"Start sync of ${collection.db}.${collection.coll}")
      generateCollectionParts(collection).map(collPart => {
        Future{collPart.sync()}
      })
    })

    Future.sequence(res)
  }

  /**
    * Return the various CollectionParts for a given Collection
    */
  def generateCollectionParts(collection: Collection): List[CollectionPart] = {
    collection.prepareSync()
  }

  /**
    * Return a Collection object for all databases
    */
  def generateCollections(): List[Collection] = {
    primary.listDatabases.flatMap(databaseName => {
      primary.listCollections(databaseName).map(collectionName => {
        new Collection(conf, databaseName, collectionName, primary, secondary)
      }).filter(x => x.db != "local" && x.coll != "oplog.rs") // TO REMOVE once we use the OplogCollectionPart
    })
  }
}
