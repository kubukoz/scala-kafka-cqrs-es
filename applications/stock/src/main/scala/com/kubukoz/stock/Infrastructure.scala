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
import natchez.Span
import natchez.TraceValue
import natchez.Kernel
import java.net.URI

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
      val ask: F[B] = fa.reader(f)
    }

  }

  // Maaaaaaybe this
  implicit def deriveFunctorTellFromAskOftell[F[_]: FlatMap, A](
    implicit ask: ApplicativeAsk[F, FunctorTell[F, A]]
  ): FunctorTell[F, A] = new DefaultFunctorTell[F, A] {
    val functor: Functor[F] = ask.applicative
    def tell(l: A): F[Unit] = ask.ask.flatMap(_.tell(l))
  }

  implicit class MapKSpan[F[_]](underlying: Span[F]) {

    def mapK[G[_]: Defer](
      fk: F ~> G
    )(
      implicit F: MonadCancelThrow[F],
      B: MonadCancelThrow[G]
    ): Span[G] = new Span[G] {
      def put(fields: (String, TraceValue)*): G[Unit] = fk(underlying.put(fields: _*))
      def kernel: G[Kernel] = fk(underlying.kernel)
      def span(name: String): Resource[G, Span[G]] = underlying.span(name).mapK(fk).map(_.mapK(fk))
      def traceId: G[Option[String]] = fk(underlying.traceId)
      def spanId: G[Option[String]] = fk(underlying.spanId)
      def traceUri: G[Option[URI]] = fk(underlying.traceUri)
    }

  }

  /////////////////////
  /////////////////////
  /////////////////////
  ///// real magic ////
  /////////////////////
  /////////////////////
  /////////////////////

  final case class Context[F[_]](session: Session[F], eventTell: FunctorTell[F, Chain[StockEvent]], span: Span[F]) {

    def mapK[G[_]: Defer](fk: F ~> G)(implicit F: MonadCancelThrow[F], G: MonadCancelThrow[G]): Context[G] =
      Context(session.mapK(fk), eventTell.mapK(fk), span.mapK(fk))
  }

  object Context {
    type Ask[F[_]] = ApplicativeAsk[F, Context[F]]
    def ask[F[_]](implicit F: Ask[F]): F[Context[F]] = F.ask
  }

  // this function basically unifies the effect in Ask[F, Context[F]]
  // given an Ask[Kleisli[F, X, *], Context[F]] for any X
  implicit def askContextInEff[F[_]: Defer: MonadCancelThrow, R](
    implicit askF: ApplicativeAsk[Kleisli[F, R, *], Context[F]]
  ): ApplicativeAsk[Kleisli[F, R, *], Context[Kleisli[F, R, *]]] = askF.map(_.mapK(Kleisli.liftK))

}
