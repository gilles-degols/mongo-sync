package controllers

import javax.inject._

import play.api.mvc._
import _root_.clone.Core
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}
import services.{Counter, MongoService}
import tools.TestRead

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

@Singleton
class StartSyncController @Inject() (cc: ControllerComponents, counter: Counter, core: Core) extends AbstractController(cc) {
  var synchroInProgress: Boolean = false
  var startedTime: Long = 0L
  val logger: Logger = LoggerFactory.getLogger("mongosync.controllers.StartSyncController")

  def sync = Action {
    if(synchroInProgress) {
      Ok("Synchronisation already started, please wait...")
    } else {
      startedTime = System.currentTimeMillis()
      synchroInProgress = true
      core.sync().onComplete {
        case Success(res) =>
          val dt = (System.currentTimeMillis() - startedTime) / 1000
          logger.info(s"Synchronisation finished in ${dt} seconds")
          synchroInProgress = false
        case Failure(err) =>
          logger.error("Failure during the synchronisation...")
          err.printStackTrace()
          synchroInProgress = false
      }

      Ok("Synchronisation started.")
    }
  }

}
