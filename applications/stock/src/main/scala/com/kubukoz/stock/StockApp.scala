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
import cats.mtl.ApplicativeAsk
import skunk.Session
import io.estatico.newtype.macros.newtype
import natchez.Trace
import cats.data.Kleisli
import cats.mtl.DefaultApplicativeAsk
import skunk.codec.numeric
import com.kubukoz.util.skunk.SkunkMiddleware

object StockApp extends IOApp {

  implicit def applicativeAskFunctor[F[_]]: Functor[ApplicativeAsk[F, ?]] = new Functor[ApplicativeAsk[F, ?]] {

    def map[A, B](fa: ApplicativeAsk[F, A])(f: A => B): ApplicativeAsk[F, B] = new DefaultApplicativeAsk[F, B] {
      val applicative: Applicative[F] = fa.applicative
      val ask: F[B]                   = fa.reader(f)
    }
  }

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

    val _ = stockEventProducer //todo

    import Trace.Implicits.noop //todo

    import cats.mtl.instances.all._
    import com.olegpy.meow.hierarchy.deriveApplicativeAsk

    type Deps   = Session[IO]
    type Eff[A] = Kleisli[IO, Deps, A]

    implicit val askF: ApplicativeAsk[Eff, Session[Eff]] = ApplicativeAsk[Eff, Deps].map(_.mapK(Kleisli.liftK))
    implicit val consoleEff: Console[Eff]                = Console.io.mapK(Kleisli.liftK)
    implicit val repository: StockRepository[Eff]        = StockRepository.instance[Eff]
    val service                                          = StockService.instance[Eff]
    val routes                                           = StockRoutes.make(service)

    val app: Resource[IO, Unit] = {
      for {
        sessionPool <- Session.pooled[IO]("localhost", user = "postgres", database = "postgres", max = 10)
        _           <- BlazeServerBuilder[IO].withHttpApp(SkunkMiddleware(sessionPool)(routes).orNotFound).resource
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

@finalAlg
trait StockRepository[F[_]] {
  def saveStock(stock: Stock): F[Stock.Id]
}

object StockRepository {
  import SessionUtils._

  def instance[F[_]: Monad: Console](implicit getSession: AskSession[F]): StockRepository[F] = {
    new StockRepository[F] {

      def saveStock(stock: Stock): F[Stock.Id] = getSession.ask.flatMap { ses =>
        import skunk.implicits._

        val action: F[Unit] =
          ses.execute(sql"select 1".query(numeric.int4)).flatMap(Console[F].putStrLn(_))

        action.as(Stock.Id(0))
      }
    }
  }
}

object SessionUtils {
  type AskSession[F[_]] = ApplicativeAsk[F, Session[F]]
}
