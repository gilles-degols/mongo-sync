package clone

import clone.structure.{Collection, CollectionPart}
import com.google.inject.Inject
import services.{ConfigurationService, MongoService}

import scala.concurrent.Future

/**
  * Handle the entire cloning of the instance
  */
class Core @Inject()(conf: ConfigurationService){

  val primary = new MongoService(conf, conf.mongoPrimaryHost)
  val secondary = new MongoService(conf, conf.mongoSecondaryHost)

  def sync(): Future[String] = {

  }

  /**
    * Return the various CollectionParts for a given Collection
    */
  def generateCollectionParts(collection: Collection): List[CollectionPart] = {
    collection.
  }

  /**
    * Return a Collection object for all databases
    */
  def generateCollections(): List[Collection] = {
    primary.listDatabases.flatMap(databaseName => {
      primary.listCollections(databaseName).map(collectionName => {
        new Collection(conf, databaseName, collectionName, primary, secondary)
      })
    })
  }
}
