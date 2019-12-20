package com.kubukoz.util.skunk

import org.http4s.HttpRoutes
import cats.data.Kleisli
import cats.data.OptionT
import skunk.Session

object SkunkMiddleware {

  type BracketThrow[F[_]] = Bracket[F, Throwable]
  type SessionK[F[_], A]  = Kleisli[F, Session[F], A]

  def apply[F[_]: BracketThrow](
    sessionPool: Resource[F, Session[F]]
  )(routes: HttpRoutes[SessionK[F, ?]]): HttpRoutes[F] = applyResource {
    sessionPool.map(Kleisli.applyK[F, Session[F]]).map(imapKRoutes(routes)(_)(Kleisli.liftK))
  }

  def imapKRoutes[F[_]: Functor, G[_]](routes: HttpRoutes[F])(to: F ~> G)(from: G ~> F): HttpRoutes[G] =
    routes.map(_.mapK(to)).mapF(_.mapK(to)).local(_.mapK(from))

  def applyResource[F[_]: BracketThrow, A, B](
    resource: Resource[F, Kleisli[OptionT[F, ?], A, B]]
  ): Kleisli[OptionT[F, ?], A, B] = Kleisli { input =>
    OptionT(resource.use(_.run(input).value))
  }
}
