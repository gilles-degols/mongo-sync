package clone.structure

import org.bson.Document
import play.api.libs.json.{JsValue, Json}
import services.{ConfigurationService, MongoService, SectionId}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
/**
  * The oplog collection is a bit different than the others as there is no _id, and we want to continuously pull from it until
  * the end of the program
  * @param conf
  * @param collection
  * @param primary
  * @param secondary
  * @param startSectionId
  * @param endSectionId
  * @param totalSections
  */
class OplogCollectionPart(conf: ConfigurationService,
                          collection: Collection,
                          primary: MongoService,
                          secondary: MongoService,
                          startSectionId: SectionId,
                          endSectionId:  SectionId,
                          totalSections:  Int) extends CollectionPart(conf, collection, primary, secondary, startSectionId, endSectionId, totalSections) {

  // Replace the previousSectionId to be able to use a JsValue
  var previousTimestamp: Option[JsValue] = None

  /**
    * There is no end to the fetching phase of the oplog. The only way to stop it, it's when the user manually ctrl+c
    * the process to remove it from maintenance.
    */
  override def continueFetching(receivedQuantity: Long, expectedQuantity: Long): Boolean = true

  override def syncSection(offset: Long, limitRead: Int, limitWrite: Int): SyncSectionResult = {
    // Fetching objects to sync from the primary. The oplog does not have any _id, nor index, so we need to use a cursor
    // as much as possible. But apparently they implemented some optimisations in the oplog to query the "ts" field fast even
    // without index.
    val query = previousTimestamp match {
      case Some(res) =>
        // previous_id is the "ts" field in this case.
        Json.obj("ts" -> Json.obj("$gt" -> res))
      case None => Json.obj()
    }

    // Read query
    val st = System.currentTimeMillis()
    val iterator = primary.findInOplog(query).iterator()
    val readTime = System.currentTimeMillis() - st

    // Writing the objects to the secondary
    // TODO: Handle timeout
    val elements = new ListBuffer[Document]()
    var i = 0
    var writeTime = System.currentTimeMillis()
    while(iterator.hasNext) {
      elements.append(iterator.next())
      i += 1
      if(i >= limitWrite) {
        val st = System.currentTimeMillis()
        insertSubset(elements.toList)
        writeTime += System.currentTimeMillis() - st
        previousTimestamp = Option((primary.toJson(elements.last) \ "ts").as[JsValue])
        elements.clear()
      }
    }

    SyncSectionResult(i, readTime = readTime, writeTime = writeTime)
  }

  override def toString: String = s"OplogCollectionPart.${db}.${coll}:[${startSectionId}.${endSectionId}]"
}
