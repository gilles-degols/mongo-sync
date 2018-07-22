package clone.structure

import java.util.concurrent.Executors

import org.bson.Document
import org.slf4j.LoggerFactory
import services.{ConfigurationService, MongoService, SectionId}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

case class SyncSectionResult(quantity: Long, readTime: Long, writeTime: Long)

/**
  *  Seeds can be None if there is no {"_id": ObjectId()} in the document database, in that case there will be only one
        thread in charge of copying the database
  * @param conf
  * @param collection
  * @param primary
  * @param secondary
  * @param startSectionId
  * @param endSectionId
  * @param totalSections
  */
abstract class CollectionPart(conf: ConfigurationService,
                     collection: Collection,
                     primary: MongoService,
                     secondary: MongoService,
                     startSectionId: SectionId,
                     endSectionId: SectionId,
                     totalSections: Int
                    ) {
  val db = collection.db
  val coll = collection.coll
  val logger = LoggerFactory.getLogger("mongosync.clone.structure.CollectionPart")

  var collectionStats: CollectionStats = primary.getCollectionStats(db, coll)
  var previousSectionId: Option[SectionId] = None

  override def toString: String = s"CollectionPart.${db}.${coll}:[${startSectionId}.${endSectionId}]"

  /**
    * In charge of syncing the entire part of the collection assigned to it, so every document between two given
        seeds. The collection must be initially created by the Collection class, this is not the job of this class.
    */
  def sync(): Unit = {
    var expectedDocuments: Long = math.max(1, collectionStats.count / totalSections) // It can increase but it's not a problem, it's only used for logging

    //  Write limit is 16MB, so we put a security factor by only using ~12 MB
    val limitWrite: Int = math.min(50000, 12 * 1024 * 1024 / math.max(1,collectionStats.avgObjSize))
    //  For the read-limit, we can arbitrarily takes up to 16 MB * 10, to avoid using too much RAM.
    val limitRead: Int = limitWrite * 10

    // Raw estimation of the data size for the current collection part
    val storageSizePart = collectionStats.storageSize / (1024*1024*1024 * totalSections.toLong)

    var fetchNextObjects = true
    var offset: Long = 0
    val st = System.currentTimeMillis()
    var readTime = 0L
    var writeTime = 0L
    var i = 0

    logger.info(s"$this (start-sync): ~$expectedDocuments docs, $storageSizePart GB.")
    while(fetchNextObjects) {
      val subSyncResult: SyncSectionResult = syncSection(offset, limitRead, limitWrite)
      offset += subSyncResult.quantity
      readTime += subSyncResult.readTime
      writeTime += subSyncResult.writeTime

      fetchNextObjects = continueFetching(subSyncResult.quantity, limitRead)

      // Display various logs
      i += 1
      if(i % 50 == 0 || true) {
        if(offset >= expectedDocuments && false) {
          // To have better logs, we check the remaining entries
          collectionStats = primary.getCollectionStats(db, coll)
          expectedDocuments = math.max(1, collectionStats.count / totalSections)
        }

        val ratio: Int = (100 * offset / expectedDocuments).toInt
        val dt = System.currentTimeMillis() - st
        var averageSpeed: Int = 1
        var expectedRemainingTime: Int = 0
        if(dt/1000 > 0 && offset / (dt / 1000) > 0) {
          averageSpeed = (offset / (dt / 1000)).toInt
          expectedRemainingTime = ((expectedDocuments - offset) / (averageSpeed * 60)).toInt // in minutes
          if(expectedRemainingTime < 0) {
            expectedRemainingTime = 0
          }
        }

        val timeLog = s"Read time ${100*readTime/dt}%, write time: ${100*writeTime/dt}%"
        logger.info(s"$this (syncing): ${offset}/$expectedDocuments docs (${ratio}%, ${averageSpeed} docs/s). Remaining time: ~${expectedRemainingTime} minutes. $timeLog")
      }
    }

    val dt = (System.currentTimeMillis() - st) / 1000L
    logger.info(s"$this (end-sync): $offset docs, ${storageSizePart}GB. Time spent: ${dt}s.")

    // We can reuse the same object as for the SyncSection.
    SyncSectionResult(offset, readTime, writeTime)
  }


  /**
    * Indicates if we should continue pulling data from the collection or not. For a BasicCollectionPart this will be easy
    * @return
    */
  def continueFetching(receivedQuantity: Long, expectedQuantity: Long): Boolean

  /**
    * Try to insert a bunch of documents, while avoiding crashes if the total size is bigger than 16MB
    */
  def insertSubset(documents: List[Document]): Unit = Future {
    /*Try{secondary.insertMany(db, coll, documents)} match {
      case Success(res) => // Nothing to do
      case Failure(err) =>
        logger.error(s"Exception while trying to insert ${documents.size} documents in $db.$coll. Try to insert them one by" +
          s" one.")
        err.printStackTrace()
        documents.foreach(doc => {
          secondary.insertMany(db, coll, List(doc))
        })
    }*/
  }

  /**
    *  In charge of syncing its part of the collection (between the two given seeds). Return the number of synced objects.
        We are not using any iterator in this case, so the method should normally not crash if the connexion is lost
        at the wrong time.
    * @param offset
    * @param limitRead
    * @param limitWrite
    */
  def syncSection(offset: Long, limitRead: Int, limitWrite: Int): SyncSectionResult
}
