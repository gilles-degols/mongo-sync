package clone.structure

import play.api.libs.json.JsObject

case class CollectionStats(raw: JsObject) {
  def avgObjSize: Int = (raw \ "avgObjSize").asOpt[Int].getOrElse(0)
  def count: Long = (raw \ "count").asOpt[Long].getOrElse(0L)
  def storageSize: Long = (raw \ "storageSize").asOpt[Long].getOrElse(0L)

  def capped: Boolean = (raw \ "capped").asOpt[Boolean].getOrElse(false)
  def max: Long = (raw \ "max").asOpt[Long].getOrElse(-1L)
  def maxSize: Long = (raw \ "maxSize").asOpt[Long].getOrElse(-1L)
}
