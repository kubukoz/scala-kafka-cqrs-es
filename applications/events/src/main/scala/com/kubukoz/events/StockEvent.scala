package com.kubukoz.events

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.Codec
import cats.mtl.FunctorTell
import cats.data.Chain
import cats.data.Kleisli

object circeConfig {
  implicit val config: Configuration = Configuration.default.withDiscriminator("@type")
}

sealed trait StockEvent extends Product with Serializable

object StockEvent {
  final case class Created(tag: String) extends StockEvent

  import circeConfig._
  import io.circe.generic.extras.semiauto._

  implicit val codec: Codec[StockEvent] = deriveConfiguredCodec

  type Write[F[_]]     = FunctorTell[F, Chain[StockEvent]]
  type WriteK[F[_], A] = Kleisli[F, Write[F], A]

  def Write[F[_]](implicit F: Write[F]): Write[F] = F

  implicit class WriteOne[F[_], G[_], V](tell: FunctorTell[F, G[V]]) {
    def tellOne(value: V)(implicit G: Applicative[G]): F[Unit] = tell.tell(G.pure(value))
  }
}
