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
import skunk.Session
import io.estatico.newtype.macros.newtype
import skunk.codec.numeric
import natchez.Trace.Implicits.noop
import com.kubukoz.util.KafkaUtils

object StockApp extends IOApp {
  import KafkaStuff._

  val server =
    for {
      producer    <- stockEventProducer[IO]
      sessionPool <- Session.pooled[IO]("localhost", user = "postgres", database = "postgres", max = 10)
      routes = StockRoutes.make[IO](sessionPool, KafkaUtils.senderResource(sendMessages(producer)))
      _ <- BlazeServerBuilder[IO].withHttpApp(routes.orNotFound).resource
    } yield ()

  def run(args: List[String]): IO[ExitCode] = server.use(_ => IO.never)

}

object KafkaStuff {
  def avroSettings[F[_]: Sync] = AvroSettings(SchemaRegistryClientSettings[F]("http://localhost:8081"))

  def stockEventProducer[F[_]: ConcurrentEffect: ContextShift] =
    Resource.liftF(avroSerializer[StockEvent].using(avroSettings).forValue).flatMap { implicit eventSerializer =>
      fs2.kafka
        .producerResource[F]
        .using(
          ProducerSettings[F, Unit, StockEvent].withBootstrapServers("localhost:9092")
        )
    }

  def sendMessages[F[_]: FlatMap](producer: KafkaProducer[F, Unit, StockEvent])(events: Chain[StockEvent]): F[Unit] = {
    val messages = ProducerRecords(events.map(event => ProducerRecord("stock-event", (), event)))

    producer.produce(messages).flatMap(_.void)
  }
}

object domain {
  final case class CreateStock(tag: String)

  final case class Stock(id: Stock.Id, tag: String)

  object Stock {

    @newtype
    final case class Id(value: Long)

    def init(tag: String) = Stock(Id(0), tag)
  }
}

import domain._

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

@finalAlg
trait StockRepository[F[_]] {
  def saveStock(stock: Stock): F[Stock.Id]
}

object StockRepository {

  def instance[F[_]: Monad](session: Session[F]): StockRepository[F] = {
    new StockRepository[F] {

      def saveStock(stock: Stock): F[Stock.Id] = {
        import skunk.implicits._

        val action: F[Unit] =
          session.execute(sql"select 1".query(numeric.int4)).void

        action.as(Stock.Id(0))
      }
    }
  }
}

object StockRoutes {

  def make[F[_]: Sync](
    //parameters being resources are request-scoped
    sessionPool: Resource[F, Session[F]],
    messageSender: StockEvent.WriteResource[F]
  ): HttpRoutes[F] = {
    val dsl = new Http4sDsl[F] {}
    import dsl._

    val mkStockService =
      (sessionPool, messageSender).tupled.map {
        case (session, sender) =>
          implicit val localSender = sender

          implicit val repo = StockRepository.instance(session)

          StockService.instance[F]
      }

    HttpRoutes.of[F] {
      case POST -> Root / "create" / tag =>
        mkStockService.use(_.create(CreateStock(tag)) *> Created())
    }
  }
}
