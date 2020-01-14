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
import skunk.codec.text
import io.circe.generic.extras.Configuration
import io.circe.Decoder
import io.circe.Encoder
import cats.mtl.ApplicativeAsk

object StockApp extends IOApp {
  import KafkaStuff._

  import Infrastructure._
  import com.olegpy.meow.hierarchy._
  import cats.mtl.instances.all._

  def mkRoutes[F[_]: Sync: Context.Ask] = StockRoutes.make[F]

  val routes = mkRoutes[Eff]

  val server =
    for {
      sessionPool <- Session.pooled[IO]("localhost", user = "postgres", database = "postgres", max = 10)
      producer    <- stockEventProducer[IO].map(sendMessages(_) _).map(KafkaUtils.senderResource(_))
      contextResource = (sessionPool.flatTap(_.transaction), producer).mapN(Context[IO])
      _ <- BlazeServerBuilder[IO].withHttpApp(runContext(contextResource)(routes).orNotFound).resource
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

    @newtype
    final case class Id(value: Long)

    object Id {

      implicit val codec: io.circe.Codec[Id] =
        io.circe.Codec.from(Decoder[Long].map(Id(_)), Encoder[Long].contramap(_.value))
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
  type SessionAsk[F[_]] = ApplicativeAsk[F, Session[F]]
  def askSession[F[_]](implicit F: SessionAsk[F]): F[Session[F]] = F.ask

  type BracketThrow[F[_]] = Bracket[F, Throwable]

  def instance[F[_]: BracketThrow: SessionAsk]: StockRepository[F] = {
    object codecs {
      val stockId = numeric.int8.imap(Stock.Id(_))(_.value)
      val stock   = (stockId ~ text.text).gimap[Stock]
    }

    new StockRepository[F] {

      def saveStock(stock: Stock): F[Stock.Id] = askSession[F].flatMap { session =>
        import skunk.implicits._

        val action: F[Unit] =
          session.prepare(sql"insert into stock(id, tag) values(${codecs.stock})".command).use(_.execute(stock)).void

        action.as(Stock.Id(0))
      }

      def retrieveStock(id: Stock.Id): F[Option[Stock]] = askSession[F].flatMap { session =>
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

  def make[F[_]: Sync: StockRepository.SessionAsk: StockEvent.Write]: HttpRoutes[F] = {
    val dsl = new Http4sDsl[F] {}
    import dsl._

    implicit val repo = StockRepository.instance[F]

    val stockService =
      StockService.instance[F]

    HttpRoutes.of[F] {
      case POST -> Root / "create" / tag =>
        stockService.create(CreateStock(tag)) *> Created()

      case GET -> Root / "findById" / id =>
        Sync[F]
          .catchNonFatal(id.toLong)
          .map(Stock.Id(_))
          .flatMap { id =>
            stockService.find(id)
          }
          .flatMap {
            case None        => NotFound()
            case Some(stock) => Ok(stock)
          }
    }
  }
}
