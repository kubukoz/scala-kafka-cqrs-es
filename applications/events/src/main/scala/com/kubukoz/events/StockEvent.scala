package com.kubukoz.events

import com.kubukoz.util._

sealed trait StockEvent extends Product with Serializable

object StockEvent extends KafkaEvent[StockEvent] {
  final case class Created(tag: String) extends StockEvent

  import vulcan.generic._
  implicit val avroCodec: vulcan.Codec[StockEvent] = vulcan.Codec.derive
}

sealed trait ReportEvent

object ReportEvent extends KafkaEvent[ReportEvent] {
  final case class Created(tag: String) extends ReportEvent

  import vulcan.generic._
  implicit val avroCodec: vulcan.Codec[ReportEvent] = vulcan.Codec.derive
}
