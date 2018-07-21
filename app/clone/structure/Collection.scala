package clone.structure

import com.google.inject.Inject
import com.mongodb.client.model.IndexOptions
import org.bson.types.ObjectId
import play.api.libs.json.{JsObject, Json}
import services.{ConfigurationService, MongoService, SectionId}

import scala.util.Try

class Collection (conf: ConfigurationService, val db: String, val coll: String, primary: MongoService, secondary: MongoService) {
  var collStats: CollectionStats = primary.getCollectionStats(db, coll)
  var previousId: Option[String] = None

  /**
    * In charge of preparing the collection to synchronize, returns the various CollectionParts needed to synchronize everything
    */
  def prepareSync(): List[CollectionPart] = {

    // Drop / create the destination collection
    checkCollection()

    // Add indexes
    copyIndexes()

    // Get various section ids
    val sectionIds = listSectionIds
    if(sectionIds.size < 2) {
      throw new Exception("We should always have at least 2 seeds")
    }

    // Create and return the list of CollectionParts for the current Collection
    var previousSectionId = sectionIds.head
    sectionIds.drop(1).map(sectionId => {
      val collPart = if(db == "local" && coll == "oplog.rs") new OplogCollectionPart(conf, this, primary, secondary, previousSectionId, sectionId, sectionIds.size)
      else new BasicCollectionPart(conf, this, primary, secondary, previousSectionId, sectionId, sectionIds.size)

      previousSectionId = sectionId
      collPart
    })
  }

  /**
    * Load the seeds we want to use for the synchronisation. Return a list of tuples, each tuple represent a range,
        the first value is the start of it, the second value, the end of it.
    * @return
    */
  def listSectionIds: List[SectionId] = {
    val firstSectionId = SectionId(Option(new ObjectId("0"*24)))
    val lastSectionId = SectionId(Option(new ObjectId("f"*24)))

    // First, we need to be sure that we have an _id and if that's an objectid, otherwise we cannot use the same technique.
    // The oplog should only be tailed by one thread at a time, so we want to be sure to never create seeds for it.
    if(Try{primary.getObjectId(primary.first(db, coll).get).get}.isFailure || (db == "local" && coll == "oplog.rs")) {
      return List(SectionId(None),SectionId(None))
    }

    // Number of seeds we would like
    val quantity = conf.maximumSeeds
    if(collStats.count <= 100 * quantity) { // Arbitrarily, we decide it's useless to use a lot of seeds if we only have a small number of documents
      return List(firstSectionId, lastSectionId)
    }

    val sectionIds = primary.sectionIds(db, coll, quantity).sortBy(_.oid.get.getTimestamp)

    // TODO: In the future, if we want to be smart and allow retry of a failed sync, we should take the previous seeds
    // stored in the mongosync database, then make a simple query to see up to where they went

    // Always add the first and last seed
    List(firstSectionId) ::: sectionIds ::: List(lastSectionId)
  }

  /**
    * Specific checks before writing to a collection
    */
  def checkCollection(): Unit = {
    // Stats about the destination collection
    var destinationStats: Option[CollectionStats] = if (secondary.listDatabases.contains(db) && secondary.listCollections(db).contains(coll)) {
      Option(secondary.getCollectionStats(db, coll))
    } else {
      None
    }

    // For the internal databases, we want to remove them by default, to avoid any problem (one exception: the oplog)
    if(destinationStats.isDefined && db == "local" && coll != "oplog.rs") { // Note: this was deactivated in python
      secondary.dropCollection(db, coll)
      destinationStats = None
    }

    if(collStats.capped && destinationStats.isEmpty) {
      var capped_max_size = collStats.maxSize
      val capped_max = collStats.max

      if(db == "local" && coll == "oplog.rs") {
        //Special case, we do not necessarily want to keep the same oplog size as the other node
        capped_max_size = conf.mongoOplogSize * 1024 * 1024 * 1024
      }

      secondary.createCollection(db, coll, true, capped_max, capped_max_size)
    }
  }

  /**
    * It is better to copy indexes directly, before copying the data. That way we directly have the TTL working, but we also
        do not need to read all data at the end to create the indexes (if Mongo is on a NFS mount, you will lose a lot of
        time just for that).
        Even if the insert performance will be "worst", at the end, it should not matter a lot with the multi-threading copy
        of mongosync
    */
  def copyIndexes(): Unit = {
    primary.getCollectionIndexes(db, coll).foreach(index => {
      val key = (index \ "key").as[JsObject]
      val options = new IndexOptions()
      // TODO: Handle the index options
      secondary.createCollectionIndex(db, coll, key, options)
    })
  }
}
