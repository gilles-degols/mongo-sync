package services

import com.mongodb._
import com.mongodb.client.{MongoClient => _, _}
import com.mongodb.client.model._
import com.mongodb.client.model.Filters._
import com.mongodb.client.model.Projections._
import java.util
import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import _root_.clone.structure.CollectionStats
import com.google.inject.{Inject, Singleton}
import org.bson.Document
import org.bson.types.ObjectId
import org.joda.time.DateTime
import play.api.Configuration
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

trait SortType {
  def json: JsObject
}
case class SortAsc() extends SortType {
  def json: JsObject = Json.obj("_id" -> 1)
}
case class SortDesc() extends SortType {
  def json: JsObject = Json.obj("_id" -> -1)
}

case class SectionId(oid: Option[ObjectId])

/**
  * We will have two instances of MongoService, as we connect to two separate databases
  * @param conf
  * @param hostname
  */
class MongoService @Inject()(conf: ConfigurationService, hostname: String) {
  val pool = Executors.newFixedThreadPool(10)
  implicit val ec =  ExecutionContext.fromExecutor(pool)
  val mongoClient: client.MongoClient = MongoClients.create(s"mongodb://$hostname")

  def toDoc(obj: JsObject): Document = {
    Document.parse(obj.toString())
  }

  def toJson(obj: Document): JsObject = {
    Json.parse(obj.toString).as[JsObject]
  }

  def getObjectId(obj: JsObject): Option[ObjectId] = {
    (obj \ "_id" \ "$oid").asOpt[String] match {
      case None => None
      case Some(oid) => Option(new ObjectId(oid))
    }
  }

  def toStringList(iterator: MongoCursor[String]): List[String] = {
    var results = List.empty[String]
    while(iterator.hasNext) {
      results = List(iterator.next()) ::: results
    }
    results
  }

  def toJsonList(iterator: MongoCursor[Document]): List[JsObject] = {
    var results = List.empty[JsObject]
    while(iterator.hasNext) {
      results = List(toJson(iterator.next())) ::: results
    }
    results
  }

  def getCollection(db: String, coll: String): MongoCollection[Document] = {
    val database: MongoDatabase = mongoClient.getDatabase(db)
    database.getCollection(coll)
  }

  /**
    * For performance purposes, we return Document and not JsObject for this method
    * @param db
    * @param coll
    * @param query
    * @return
    */
  def find(db: String, coll: String, query: JsObject, skip: Int, limit: Int, projection: JsObject, sortType: SortType): Future[FindIterable[Document]] = Future {
    getCollection(db, coll).find(toDoc(query)).skip(skip).limit(limit).projection(toDoc(projection)).sort(toDoc(sortType.json))
  }

  def findInOplog(db: String, coll: String, query: JsObject): Future[FindIterable[Document]] = Future {
    getCollection(db, coll).find(toDoc(query)).cursorType(CursorType.TailableAwait)
  }

  def findOne(db: String, coll: String, query: JsObject, sortType: SortType): Option[JsObject] = {
    toJsonList(Await.result(find(db, coll, query, skip = 0, limit = 1, projection = Json.obj(), sortType = sortType), Duration.Inf).iterator()).headOption
  }

  def first(db: String, coll: String): Option[JsObject] = {
    toJsonList(Await.result(find(db, coll, Json.obj(), skip = 0, limit = 1, projection = Json.obj(), sortType = SortAsc()), Duration.Inf).iterator()).headOption
  }

  def listDatabases: List[String] = {
    toStringList(mongoClient.listDatabaseNames().iterator())
  }

  def listCollections(db: String): List[String] = {
    toStringList(mongoClient.getDatabase(db).listCollectionNames().iterator())
  }

  def getCollectionIndexes(db: String, coll: String): List[JsObject] = {
    toJsonList(getCollection(db, coll).listIndexes().iterator())
  }

  def createCollection(db: String, coll: String, capped: Boolean, max: Long, maxSize: Long) = {
    val options = new CreateCollectionOptions()
    options.capped(capped)
    if(max > 0) options.maxDocuments(max)
    if(maxSize > 0) options.sizeInBytes(maxSize)
    mongoClient.getDatabase(db).createCollection(coll, options)
  }

  def createCollectionIndex(db: String, coll: String, key: JsObject, options: IndexOptions): String = {
    getCollection(db, coll).createIndex(toDoc(key), options)
  }

  def getCollectionStats(db: String, coll: String): CollectionStats = {
    CollectionStats(toJson(mongoClient.getDatabase(db).runCommand(new Document("collStats", coll))))
  }

  def dropCollection(db: String, coll: String) = {
    getCollection(db, coll).drop()
  }

  /**
    * For performance purposes, we expect Document and not JsObject
    * @param db
    * @param coll
    * @param docs
    */
  def insertMany(db: String, coll: String, docs: List[Document]): Future[Unit] = Future {
    val options = new InsertManyOptions()
    options.ordered(false)
    options.bypassDocumentValidation(true)
    getCollection(db, coll).insertMany(docs.asJava, options)
  }

  /**
    * A list of ids from a given collection, which could be use to "equally" split the collection. Return an empty list if not possible to do it.
        Duplicate seeds are possible. _ids are not returned in any specific order.
    * @param db
    * @param coll
    * @param quantity
    */
  def sectionIds(db: String, coll: String, quantity: Int): List[SectionId] = {
    // The "$sample" is slow, so we will try to generate random object ids ourselves
    var result = List.empty[SectionId]

    //We want to have some information about the smallest and biggest _id (we suppose that the _id is monotonously increasing)
    val first: Option[JsObject] = findOne(db, coll, Json.obj(), sortType = SortAsc())
    val last: Option[JsObject] = findOne(db, coll, Json.obj(), sortType = SortDesc())
    if(first.isEmpty || last.isEmpty || getObjectId(first.get).isEmpty || getObjectId(last.get).isEmpty) {
      return result
    }

    // Arbitrarily generate object ids between the minimal/maximal values
    val firstTimestamp = getObjectId(first.get).get.getTimestamp
    val lastTimestamp = getObjectId(last.get).get.getTimestamp
    val step: Int = math.max(lastTimestamp - firstTimestamp, 1)

    for(offset <- firstTimestamp to lastTimestamp by step) {
      val currentDate = new DateTime(offset)
      val sectionId = SectionId(oid = getObjectId(first.get))
      result = result ::: List(sectionId)
    }

    result
  }
}
