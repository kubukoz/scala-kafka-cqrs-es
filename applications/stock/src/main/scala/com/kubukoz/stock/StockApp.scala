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
import cats.mtl.DefaultApplicativeAsk
import skunk.codec.numeric
import cats.tagless.autoFunctorK

object StockApp extends IOApp {

  implicit def applicativeAskFunctor[F[_]]: Functor[ApplicativeAsk[F, ?]] = new Functor[ApplicativeAsk[F, ?]] {

    def map[A, B](fa: ApplicativeAsk[F, A])(f: A => B): ApplicativeAsk[F, B] = new DefaultApplicativeAsk[F, B] {
      val applicative: Applicative[F] = fa.applicative
      val ask: F[B]                   = fa.reader(f)
    }
  }

  import com.olegpy.meow.effects._

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

    import natchez.Trace.Implicits.noop

    val app: Resource[IO, Unit] = {
      for {
        producer    <- stockEventProducer
        sessionPool <- Session.pooled[IO]("localhost", user = "postgres", database = "postgres", max = 10)
        routes = StockRoutes.make[IO](sessionPool, senderResource(producer))
        _ <- BlazeServerBuilder[IO].withHttpApp(routes.orNotFound).resource
      } yield ()
    }

    app.use(_ => IO.never)
  } as ExitCode.Success

  def senderResource[F[_]: Sync](producer: KafkaProducer[F, Unit, StockEvent]): Resource[F, StockEvent.Write[F]] =
    Resource
      .makeCase(Ref[F].of(Chain.empty[StockEvent])) {
        case (ref, ExitCase.Completed) => ref.get.flatMap(sendMessages(producer))
        case _                         => ().pure[F]
      }
      .map(_.tellInstance)

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

  def instance[F[_]: StockRepository: StockEvent.Write: FlatMap]: StockService[F] = new StockService[F] {

    def create(stock: CreateStock): F[Stock.Id] =
      StockRepository[F].saveStock(Stock.init(stock.tag)) <* StockEvent.Write[F].tellOne(StockEvent.Created(stock.tag))
  }
}

object StockRoutes {

  type MessageSenderResource[F[_]] = Resource[F, StockEvent.Write[F]]

  def make[F[_]: Sync](
    sessionPool: Resource[F, Session[F]],
    messageSender: MessageSenderResource[F]
  ): HttpRoutes[F] = {
    val dsl = new Http4sDsl[F] {}
    import dsl._

    val mkStockService = (sessionPool, messageSender).tupled.map {
      case (session, sender) =>
        implicit val senderImplicit                         = sender
        implicit val askSession: SessionUtils.AskSession[F] = ApplicativeAsk.const(session)

        implicit val repo = StockRepository.instance[F]

        StockService.instance[F]
    }

    HttpRoutes.of[F] {
      case POST -> Root / "create" / tag =>
        mkStockService.use(_.create(CreateStock(tag)) *> Created())
    }
  }
}

import java.lang.SuppressWarnings

@finalAlg
@autoFunctorK
trait StockRepository[F[_]] {
  def saveStock(stock: Stock): F[Stock.Id]
}

object StockRepository {
  import SessionUtils._

  def instance[F[_]: Monad](implicit getSession: AskSession[F]): StockRepository[F] = {
    new StockRepository[F] {

      def saveStock(stock: Stock): F[Stock.Id] = getSession.ask.flatMap { ses =>
        import skunk.implicits._

        val action: F[Unit] =
          ses.execute(sql"select 1".query(numeric.int4)).void

        action.as(Stock.Id(0))
      }
    }
  }
}

object SessionUtils {
  type AskSession[F[_]] = ApplicativeAsk[F, Session[F]]
}
