package com.kubukoz.stock

import com.kubukoz.events._
import cats.data.Chain
import org.http4s.HttpRoutes
import org.http4s._
import skunk.Session
import cats.data.Kleisli
import cats.mtl.FunctorTell
import cats.mtl.DefaultFunctorTell
import cats.mtl.ApplicativeAsk
import cats.mtl.DefaultApplicativeAsk

//A bunch of weird things
object Infrastructure {

  implicit class MapKFunctorTell[F[_], A](ft: FunctorTell[F, A]) {

    def mapK[G[_]: Functor](fk: F ~> G): FunctorTell[G, A] = new DefaultFunctorTell[G, A] {
      val functor: Functor[G] = Functor[G]
      def tell(l: A): G[Unit] = fk(ft.tell(l))
    }
  }

  // I swear, I'll get this merged in cats...
  implicit def applicativeAskFunctor[F[_]]: Functor[ApplicativeAsk[F, *]] = new Functor[ApplicativeAsk[F, *]] {

    def map[A, B](fa: ApplicativeAsk[F, A])(f: A => B): ApplicativeAsk[F, B] = new DefaultApplicativeAsk[F, B] {
      val applicative: Applicative[F] = fa.applicative
      val ask: F[B]                   = fa.reader(f)
    }
  }

  // Maybe this as well...
  implicit final class ResourceApplyKleisli[F[_], A](private val res: Resource[F, A]) extends AnyVal {

    def runKleisli(implicit B: Bracket[F, Throwable]): Kleisli[F, A, *] ~> F =
      λ[Kleisli[F, A, *] ~> F](eff => res.use(eff.run))
  }

  // Maaaaaaybe this
  implicit def deriveFunctorTellFromAskOftell[F[_]: FlatMap, A](
    implicit ask: ApplicativeAsk[F, FunctorTell[F, A]]
  ): FunctorTell[F, A] = new DefaultFunctorTell[F, A] {
    val functor: Functor[F] = ask.applicative
    def tell(l: A): F[Unit] = ask.ask.flatMap(_.tell(l))
  }

  /////////////////////
  /////////////////////
  /////////////////////
  ///// real magic ////
  /////////////////////
  /////////////////////
  /////////////////////

  final case class Context[F[_]](session: Session[F], eventTell: FunctorTell[F, Chain[StockEvent]]) {

    def mapK[G[_]: Applicative: Defer](fk: F ~> G)(implicit F: Bracket[F, Throwable]): Context[G] = {
      Context(session.mapK(fk), eventTell.mapK(fk))
    }
  }

  object Context {
    type Ask[F[_]] = ApplicativeAsk[F, Context[F]]
    def ask[F[_]](implicit F: Ask[F]): F[Context[F]] = F.ask
  }

  // todo fiddle around with Sync constraint
  // this function basically unifies the effect in Ask[F, Context[F]]
  // given an Ask[Kleisli[F, X, *], Context[F]] for any X
  implicit def askContextInEff[F[_]: Sync, R](
    implicit askF: ApplicativeAsk[Kleisli[F, R, *], Context[F]]
  ): ApplicativeAsk[Kleisli[F, R, *], Context[Kleisli[F, R, *]]] = askF.map(_.mapK(Kleisli.liftK))

  type Eff[A] = Kleisli[IO, Context[IO], A]

  // Shady shit, don't touch
  // Basically: run the request handling function in one resource, then run the response stream in another one.
  // http4s's API doesn't allow mixing them that easily
  // (there could be some plumbing done with resource.allocated and friends but it's too shady even for me)
  // so here's that
  //
  // tl;dr: pass a resource and it'll be used on every request to translate the effect
  // from Kleisli of the resource to the underlying effect.
  def runContext[F[_]: Concurrent, Ctx](
    contextResource: Resource[F, Ctx]
  )(routesEff: HttpRoutes[Kleisli[F, Ctx, *]]): HttpRoutes[F] =
    routesEff.local[Request[F]](_.mapK(Kleisli.liftK)).mapF(_.mapK(contextResource.runKleisli)).map { response =>
      val transactedBody =
        fs2.Stream
          .resource(contextResource)
          .map(Kleisli.applyK[F, Ctx](_))
          .flatMap(response.body.translateInterruptible(_))

      response.copy(body = transactedBody)
    }

}
