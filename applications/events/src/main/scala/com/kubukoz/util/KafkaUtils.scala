package com.kubukoz.util

import cats.mtl.FunctorTell
import cats.data.Chain
import cats.mtl.DefaultFunctorTell

object KafkaUtils {

  def refTell[F[_]: Functor, A: Monoid](ref: Ref[F, A]): FunctorTell[F, A] = new DefaultFunctorTell[F, A] {
    val functor: Functor[F] = implicitly

    def tell(l: A): F[Unit] = ref.update(_ |+| l)

  }

  def senderResource[F[_]: Ref.Make: Monad, Log: Monoid](sendMessages: Log => F[Unit]): Resource[F, FunctorTell[F, Log]] =
    Resource
      .makeCase(Ref[F].of(Monoid[Log].empty)) {
        case (ref, kernel.Resource.ExitCase.Succeeded) => ref.get.flatMap(sendMessages)
        case _                                         => ().pure[F]
      }
      .map(refTell(_))

}

trait KafkaEvent[E] {

  type Write[F[_]] = FunctorTell[F, Chain[E]]
  type WriteResource[F[_]] = Resource[F, Write[F]]

  def Write[F[_]](implicit F: Write[F]): Write[F] = F

  implicit class WriteOne[F[_], G[_], V](tell: FunctorTell[F, G[V]]) {
    def tellOne(value: V)(implicit G: Applicative[G]): F[Unit] = tell.tell(G.pure(value))
  }

}
