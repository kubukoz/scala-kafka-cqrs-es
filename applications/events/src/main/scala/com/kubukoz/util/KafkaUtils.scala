package com.kubukoz.util

import org.http4s.Request
import org.http4s.HttpRoutes
import cats.data.Kleisli
import cats.mtl.FunctorTell
import cats.data.OptionT
import cats.mtl.DefaultFunctorTell

object KafkaUtils {

  object WriterSenderMiddleware {
    import com.olegpy.meow.effects._

    def apply[F[_]: Sync, Logs: Monoid](send: Logs => F[Unit])(
      ): HttpRoutes[Kleisli[F, FunctorTell[F, Logs], ?]] => HttpRoutes[F] =
      _.local[Request[F]](_.mapK(Kleisli.liftK)).mapF { underlying =>
        OptionT.liftF(Ref[F].of(Monoid[Logs].empty)).flatMap { ref =>
          val run = Kleisli.applyK[F, FunctorTell[F, Logs]](ref.tellInstance)

          underlying.mapK(run).map(_.mapK(run)) <* OptionT.liftF(ref.get.flatMap(send))
        }
      }
  }

  implicit def deriveTellFromKleisliOfTell[F[_]: Functor, Log]: FunctorTell[Kleisli[F, FunctorTell[F, Log], ?], Log] =
    new DefaultFunctorTell[Kleisli[F, FunctorTell[F, Log], ?], Log] {
      val functor: Functor[Kleisli[F, FunctorTell[F, Log], ?]] = Functor[Kleisli[F, FunctorTell[F, Log], ?]]

      def tell(l: Log): Kleisli[F, FunctorTell[F, Log], Unit] = Kleisli { _.tell(l) }
    }
}
