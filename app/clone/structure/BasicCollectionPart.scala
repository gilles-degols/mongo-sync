package clone.structure

import org.bson.Document
import org.bson.types.ObjectId
import play.api.libs.json.{JsObject, Json}
import services.{ConfigurationService, MongoService, SectionId, SortAsc}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

class BasicCollectionPart(conf: ConfigurationService,
                          collection: Collection,
                          primary: MongoService,
                          secondary: MongoService,
                          startSectionId: SectionId,
                          endSectionId:  SectionId,
                          totalSections:  Int) extends CollectionPart(conf, collection, primary, secondary, startSectionId, endSectionId, totalSections) {

  override def continueFetching(receivedQuantity: Long, expectedQuantity: Long): Boolean = receivedQuantity >= expectedQuantity

  override def syncSection(offset: Long, limitRead: Int, limitWrite: Int): SyncSectionResult = {
    // Fetching objects to sync from the primary. Not all documents have a "_id" field, so be careful. Performance will be very
    // slow if you do not have it, and it might crash if there are too many documents without any "_id" (TODO we should switch
    // to cursors in that case). We assume that if a collection contains a document with "_id", all documents have it.
    var queryId = Json.obj()
    if(startSectionId.oid.isDefined) queryId = queryId ++ Json.obj("$gte" -> startSectionId.toQuery)
    if(endSectionId.oid.isDefined) queryId = queryId ++ Json.obj("$lte" -> endSectionId.toQuery) // To be sure the final item, it's better to do one extra insert each time

    if(previousSectionId.isDefined) {
      if(previousSectionId.get.oid.isDefined) queryId = queryId ++ Json.obj("$gte" -> previousSectionId.get.toQuery)
      else if(previousSectionId.get.other.isDefined) queryId = queryId ++ Json.obj("$gte" -> previousSectionId.get.other.get)
    }

    var skip: Long = 0
    var query = Json.obj("_id" -> queryId)
    if((startSectionId.oid.isEmpty || endSectionId.oid.isEmpty) && previousSectionId.isEmpty) {
      //  Special code to handle collection without any _id at all, or at the first iteration
      query = Json.obj()
      skip = offset
    }

    // Load results (cursor)
    var st = System.currentTimeMillis()
    var readTime: Long = 0L
    var writeTime: Long = 0L
    val iterator = primary.find(db, coll, query, skip=skip.toInt, limit = limitRead, projection = Json.obj(), sortType = SortAsc()).iterator()

    // Reading all elements
    val elements = new ListBuffer[Document]()

    while(iterator.hasNext) {
      elements.append(iterator.next())
    }
    readTime += System.currentTimeMillis() - st

    val docs = elements.toList

    // Writing all elements
    st = System.currentTimeMillis()
    docs.sliding(limitWrite, limitWrite).foreach(subset => insertSubset(subset))
    writeTime += System.currentTimeMillis() - st

    // Now we can assume that we correctly inserted the expected documents, so we can store the new start for the section
    // to copy. We assume to only have a String if the _id is not an ObjectId
    if(docs.nonEmpty && docs.last.containsKey("_id")) {
      val lastDoc: JsObject = primary.toJson(docs.last)
      (lastDoc \ "_id" \ "$oid").asOpt[String] match {
        case Some(oid) => previousSectionId = Option(SectionId(oid = Option(new ObjectId(oid))))
        case None => previousSectionId = Option(SectionId(oid = None, other = Option((lastDoc \ "_id").as[String])))
      }
    }

    SyncSectionResult(docs.length.toLong, readTime = readTime, writeTime = writeTime)
  }

  override def toString: String = {
    val start = startSectionId.oid match {
      case Some(res) => res.toHexString
      case None => startSectionId.other.getOrElse("None")
    }
    val end = endSectionId.oid match {
      case Some(res) => res.toHexString
      case None => endSectionId.other.getOrElse("None")
    }
    s"BasicCollectionPart.${db}.${coll}:[${start}.${end}]"
  }
}
