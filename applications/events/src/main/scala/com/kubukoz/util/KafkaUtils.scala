package com.kubukoz.util

import cats.mtl.FunctorTell
import com.olegpy.meow.effects._
import cats.data.Chain

object KafkaUtils {

  def senderResource[F[_]: Sync, Log: Monoid](sendMessages: Log => F[Unit]): Resource[F, FunctorTell[F, Log]] =
    Resource
      .makeCase(Ref[F].of(Monoid[Log].empty)) {
        case (ref, ExitCase.Completed) => ref.get.flatMap(sendMessages)
        case _                         => ().pure[F]
      }
      .map(_.tellInstance)
}

trait KafkaEvent[E] {

  type Write[F[_]]         = FunctorTell[F, Chain[E]]
  type WriteResource[F[_]] = Resource[F, Write[F]]

  def Write[F[_]](implicit F: Write[F]): Write[F] = F

  implicit class WriteOne[F[_], G[_], V](tell: FunctorTell[F, G[V]]) {
    def tellOne(value: V)(implicit G: Applicative[G]): F[Unit] = tell.tell(G.pure(value))
  }
}
