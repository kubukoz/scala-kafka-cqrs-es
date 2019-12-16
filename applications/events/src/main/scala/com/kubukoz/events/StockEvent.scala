package com.kubukoz.events

import cats.mtl.FunctorTell
import cats.data.Chain
import cats.data.Kleisli

sealed trait StockEvent extends Product with Serializable

object StockEvent {
  final case class Created(tag: String) extends StockEvent

  import vulcan.generic._
  implicit val avroCodec: vulcan.Codec[StockEvent] = vulcan.Codec.derive

  type Write[F[_]]     = FunctorTell[F, Chain[StockEvent]]
  type WriteK[F[_], A] = Kleisli[F, Write[F], A]

  def Write[F[_]](implicit F: Write[F]): Write[F] = F

  implicit class WriteOne[F[_], G[_], V](tell: FunctorTell[F, G[V]]) {
    def tellOne(value: V)(implicit G: Applicative[G]): F[Unit] = tell.tell(G.pure(value))
  }
}
