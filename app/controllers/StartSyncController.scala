package controllers

import javax.inject._

import play.api.mvc._
import _root_.clone.Core
import org.slf4j.{Logger, LoggerFactory}
import services.{Counter, MongoService}
import tools.TestRead

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

@Singleton
class StartSyncController @Inject() (cc: ControllerComponents, counter: Counter, core: Core) extends AbstractController(cc) {
  var synchroInProgress: Boolean = false
  val logger: Logger = LoggerFactory.getLogger("net.degols.mongosync.app.controllers.StartSyncController")

  def count = Action {
    if(synchroInProgress) {
      Ok("Synchronisation already started, please wait...")
    } else {
      synchroInProgress = true
      core.sync().onComplete {
        case Success(res) =>
          logger.info(s"Synchronisation finished: ${res}")
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
