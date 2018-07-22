package tools

import com.google.inject.Inject
import org.slf4j
import org.slf4j.LoggerFactory
import play.api.libs.json.Json
import services.{ConfigurationService, MongoService, SortAsc}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TestRead(conf: ConfigurationService, mongoService: MongoService) {
  final private val logger: slf4j.Logger = LoggerFactory.getLogger("mongosync.tools.TestRead")

  def start(): Future[String] = Future {
    logger.debug("Start TestRead")
    val st = System.currentTimeMillis()
    var n = 0

    val iterator = mongoService.find(conf.internalDatabase, conf.testWriteCollection, query = Json.obj(), projection=Json.obj(), skip = 0, limit = 0, sortType = SortAsc()).iterator()
    while(iterator.hasNext) {
      n += 1
      iterator.next()
    }

    val dt = System.currentTimeMillis() - st
    val ratio = (n / (dt / 1000)).toInt
    s"End TestRead after $dt ms ($ratio docs/s)"
  }
}
