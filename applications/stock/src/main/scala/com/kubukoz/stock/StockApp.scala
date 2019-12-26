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
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import skunk.codec.text
import skunk.Codec
import io.circe.generic.extras.Configuration

object StockApp extends IOApp {
  import KafkaStuff._

  val logger = Slf4jLogger.getLogger[IO]

  def logMessages(events: Chain[StockEvent]): IO[Unit] =
    logger.info("Sending messages: " + events.map(_.toString).mkString_(", "))

  val server =
    for {
      sessionPool <- Session.pooled[IO]("localhost", user = "postgres", database = "postgres", max = 10)
      (producer: StockEvent.WriteResource[IO]) = KafkaUtils.senderResource(logMessages)
      // (producer: StockEvent.WriteResource[IO]) <- stockEventProducer[IO]
      //   .map(sendMessages(_) _)
      //   .map(KafkaUtils.senderResource(_))
      routes = StockRoutes.make[IO](sessionPool.flatTap(_.transaction), producer)
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
    import io.circe.generic.extras.semiauto._

    implicit val config = Configuration.default

    final case class Id(value: Long) extends AnyVal

    object Id {
      implicit val codec: io.circe.Codec[Id] = deriveUnwrappedCodec
    }

    def init(tag: String) = Stock(Id(0), tag)

    implicit val codec: io.circe.Codec[Stock] = deriveConfiguredCodec
  }
}

import domain._

@finalAlg
trait StockService[F[_]] {
  def create(stock: CreateStock): F[Stock.Id]
  def find(id: Stock.Id): F[Option[Stock]]
}

object StockService {

  def instance[F[_]: StockRepository: StockEvent.Write: FlatMap]: StockService[F] = new StockService[F] {

    def create(stock: CreateStock): F[Stock.Id] =
      StockRepository[F].saveStock(Stock.init(stock.tag)) <* StockEvent.Write[F].tellOne(StockEvent.Created(stock.tag))

    def find(id: Stock.Id): F[Option[Stock]] = StockRepository[F].retrieveStock(id)
  }
}

@finalAlg
trait StockRepository[F[_]] {
  def saveStock(stock: Stock): F[Stock.Id]
  def retrieveStock(id: Stock.Id): F[Option[Stock]]
}

object StockRepository {

  type BracketThrow[F[_]] = Bracket[F, Throwable]

  def instance[F[_]: BracketThrow](session: Session[F]): StockRepository[F] = {
    object codecs {
      val stockId = numeric.int8.gimap[Stock.Id]
      val stock   = (stockId ~ text.text).gimap[Stock]
    }

    new StockRepository[F] {

      def saveStock(stock: Stock): F[Stock.Id] = {
        import skunk.implicits._

        val action: F[Unit] =
          session.prepare(sql"insert into stock(id, tag) values(${codecs.stock})".command).use(_.execute(stock)).void

        action.as(Stock.Id(0))
      }

      def retrieveStock(id: Stock.Id): F[Option[Stock]] = {
        import skunk.implicits._

        session
          .prepare(sql"select id, tag from stock where id = ${codecs.stockId}".query(codecs.stock))
          .use(_.option(id))
      }
    }
  }
}

object StockRoutes {

  import org.http4s.circe.CirceEntityCodec._

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

      case GET -> Root / "findById" / id =>
        Sync[F]
          .catchNonFatal(id.toLong)
          .map(Stock.Id(_))
          .flatMap { id =>
            mkStockService.use(_.find(id))
          }
          .flatMap {
            case None        => NotFound()
            case Some(stock) => Ok(stock)
          }
    }
  }
}
