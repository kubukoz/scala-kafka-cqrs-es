package com.kubukoz.stock

import com.kubukoz.events._
import fs2.kafka.KafkaProducer
import fs2.kafka.ProducerRecord
import fs2.kafka.ProducerRecords
import cats.data.Chain
import cats.tagless.finalAlg
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import fs2.kafka.ProducerSettings
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.implicits._
import org.http4s._
import fs2.kafka.vulcan._
import com.kubukoz.util.KafkaUtils._
import cats.mtl.ApplicativeAsk
import skunk.Session
import io.circe.Json
import io.estatico.newtype.macros.newtype
import natchez.Trace
import cats.data.Kleisli
import cats.mtl.DefaultApplicativeAsk
import cats.tagless.syntax.`package`.functorK
import cats.tagless.autoFunctorK
import com.kubukoz.stock.SessionUtils.GetSession
import cats.data.OptionT
import skunk.codec.numeric

object StockApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val avroSettings = AvroSettings(SchemaRegistryClientSettings[IO]("http://localhost:8081"))

    val stockEventProducer =
      Resource.liftF(avroSerializer[StockEvent].using(avroSettings).forValue).flatMap { implicit eventSerializer =>
        fs2.kafka
          .producerResource[IO]
          .using(
            ProducerSettings[IO, Unit, StockEvent].withBootstrapServers("localhost:9092")
          )
      }

    import Trace.Implicits.noop //todo

    import cats.mtl.instances.all._
    import com.olegpy.meow.hierarchy.deriveApplicativeAsk

    val app: Resource[IO, Unit] = {

      type Eff[A] = Kleisli[IO, Session[IO], A]

      def middleware(sessionPool: Resource[IO, Session[IO]])(routes: HttpRoutes[Eff]): HttpRoutes[IO] = HttpRoutes {
        request =>
          implicit val b: Bracket[OptionT[IO, ?], Throwable] = Sync[OptionT[IO, ?]]

          sessionPool.mapK(OptionT.liftK).use { session =>
            routes.run(request.mapK(Kleisli.liftK)).map(_.mapK(Kleisli.applyK(session))).mapK(Kleisli.applyK(session))
          }
      }

      implicit val consoleEff: Console[Eff] = Console.io.mapK(Kleisli.liftK)

      for {
        sessionPool <- Session.pooled[IO]("localhost", user = "postgres", database = "postgres", max = 10)

        implicit0(repository: StockRepository[Eff]) = {

          val get: GetSession[Eff, IO] = GetSession(implicitly[ApplicativeAsk[Eff, Session[IO]]])

          implicit val askF: ApplicativeAsk[Eff, Session[Eff]] = get.mappedSession(Kleisli.liftK)

          StockRepository.instance[Eff]
        }

        service = StockService.instance[Eff]
        routes  = StockRoutes.make(service)

        _ <- BlazeServerBuilder[IO].withHttpApp(middleware(sessionPool)(routes).orNotFound).resource
      } yield ()
    }

    app.use(_ => IO.never)
  } as ExitCode.Success

  def sendMessages[F[_]: FlatMap](producer: KafkaProducer[F, Unit, StockEvent])(events: Chain[StockEvent]): F[Unit] = {
    val messages = ProducerRecords(events.map(event => ProducerRecord("stock-event", (), event)))

    producer.produce(messages).flatMap(_.void)
  }
}

final case class CreateStock(tag: String)

final case class Stock(id: Stock.Id, tag: String)

object Stock {

  @newtype
  final case class Id(value: Long)

  def init(tag: String) = Stock(Id(0), tag)
}

@finalAlg
trait StockService[F[_]] {
  def create(stock: CreateStock): F[Stock.Id]
}

object StockService {

  def instance[F[_]: StockRepository: FlatMap]: StockService[F] = new StockService[F] {

    def create(stock: CreateStock): F[Stock.Id] =
      StockRepository[F].saveStock(Stock.init(stock.tag))
    //  <*  StockEvent
    //   .Write[F]
    //   .tellOne(StockEvent.Created(stock.tag))
    //   .as(??? : Stock.Id)
  }
}

object StockRoutes {

  def make[F[_]: Sync](stockService: StockService[F]): HttpRoutes[F] = {
    val dsl = new Http4sDsl[F] {}
    import dsl._

    HttpRoutes.of[F] {
      case POST -> Root / "create" / tag =>
        stockService.create(CreateStock(tag)) *> Created()
    }
  }
}

import SessionUtils._

import cats.tagless.implicits._

@finalAlg
trait StockRepository[F[_]] {
  def saveStock(stock: Stock): F[Stock.Id]
}

object StockRepository {

  def instance[F[_]: Monad: Console](implicit getSession: AskSession[F]): StockRepository[F] = {
    new StockRepository[F] {

      def saveStock(stock: Stock): F[Stock.Id] = getSession.ask.flatMap { ses =>
        // do something with session
        import skunk.implicits._

        val a: F[Unit] =
          ses.execute(sql"select 1".query(numeric.int4)).flatMap(Console[F].putStrLn(_))

        a.as(Stock.Id(0))
      }
    }
  }
}

object SessionUtils {

  type AskSession[F[_]] = ApplicativeAsk[F, Session[F]]

  final case class GetSession[F[_]: Defer: Applicative, G[_]: Bracket[?[_], Throwable]](
    underlying: ApplicativeAsk[F, Session[G]]
  ) {

    def mappedSession(lift: G ~> F): ApplicativeAsk[F, Session[F]] = new DefaultApplicativeAsk[F, Session[F]] {
      val applicative: Applicative[F] = Applicative[F]
      val ask: F[Session[F]]          = underlying.ask.map(_.mapK(lift))
    }
  }
}
