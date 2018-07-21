package tools

import com.google.inject.Inject
import org.slf4j
import org.slf4j.LoggerFactory
import play.api.libs.json.Json
import services.MongoService

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TestRead @Inject()(mongoService: MongoService) {
  final private val logger: slf4j.Logger = LoggerFactory.getLogger("mosync.services.TestRead")

  def start(): Future[String] = {
    logger.debug("Start TestRead")
    val st = System.currentTimeMillis()
    var n = 0

    val result: Future[String] = mongoService.find("mongosync", "testWrite", Json.obj()).map(raw => {
      val iterator = raw.iterator()
      while(iterator.hasNext) {
        n += 1
        iterator.next()
      }

      val dt = System.currentTimeMillis() - st
      val ratio = (n / (dt / 1000)).toInt
      s"End TestRead after $dt ms ($ratio docs/s)"
    }).map(x => {
      logger.debug(x)
      x
    })

    result
  }
}
