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
import natchez.Trace
import com.kubukoz.util.KafkaUtils
import skunk.codec.text
import io.circe.generic.extras.Configuration
import io.circe.Decoder
import io.circe.Encoder
import cats.mtl.ApplicativeAsk
import cats.mtl.FunctorTell
import cats.data.Kleisli
import natchez.Span
import natchez.TraceValue
import natchez.Kernel
import natchez.jaeger.Jaeger
import org.http4s.util.CaseInsensitiveString
import natchez.EntryPoint

object StockApp extends IOApp {
  import KafkaStuff._

  import Infrastructure._
  import com.olegpy.meow.hierarchy._
  import cats.mtl.instances.all._

  def mkRoutes[F[_]: Sync: Context.Ask: Trace] = {
    implicit val repo    = StockRepository.instance[F]
    implicit val service = StockService.instance[F]
    StockRoutes.make[F]
  }

  type Eff[A] = Kleisli[IO, Context[IO], A]

  type EffTrace[A] = Kleisli[IO, Span[IO], A]

  implicit val effTrace: Trace[Eff] = Trace.kleisliInstance[IO].lens(_.span, (ctx, span) => ctx.copy(span = span))
  val routes                        = mkRoutes[Eff]

  def requestToSpan[F[_]](ep: EntryPoint[F], r: Request[F]): Resource[F, Span[F]] = {
    r.headers.get(CaseInsensitiveString("X-B3-TraceId")) match {
      case Some(traceId) => ep.continue("request" + r.pathInfo, Kernel(Map("X-B3-TraceId" -> traceId.value)))
      case None          => ep.root("request" + r.pathInfo)
    }
  }

  val server =
    for {
      jaeger <- Jaeger.entryPoint[IO]("stock-app") { c =>
        import io.jaegertracing.Configuration.SamplerConfiguration
        import io.jaegertracing.Configuration.ReporterConfiguration
        IO(c.withSampler(SamplerConfiguration.fromEnv).withReporter(ReporterConfiguration.fromEnv).getTracer())
      }

      //replace with session.pooled when https://github.com/tpolecat/skunk/issues/124 is resolved
      sessionPool <- Resource.pure[IO, Resource[EffTrace, Session[EffTrace]]](
        Session.single[EffTrace]("localhost", user = "postgres", database = "postgres")
      )

      producer <- stockEventProducer[IO].map(sendMessages(_) _).map(KafkaUtils.senderResource(_))

      _ <- BlazeServerBuilder[IO]
        .withHttpApp(HttpApp[IO] { req =>
          val contextResource = requestToSpan(jaeger, req).flatMap { span =>
            val tracedSession =
              sessionPool.flatTap(_.transaction).mapK(Kleisli.applyK(span)).map(_.mapK(Kleisli.applyK(span)))

            (tracedSession, producer).mapN(Context[IO](_, _, span))
          }

          contextResource.use { ctx =>
            runContext(Resource.pure[IO, Context[IO]](ctx))(routes).orNotFound.run(req)
          }
        })
        .resource
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

  def instance[F[_]: StockRepository: StockEvent.Write: FlatMap: Trace]: StockService[F] = new StockService[F] {

    def create(stock: CreateStock): F[Stock.Id] =
      Trace[F].span("bamboozled") {
        StockRepository[F]
          .saveStock(Stock.init(stock.tag)) <* StockEvent.Write[F].tellOne(StockEvent.Created(stock.tag))
      }

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

  def make[F[_]: Sync: StockService]: HttpRoutes[F] = {
    val dsl = new Http4sDsl[F] {}
    import dsl._

    HttpRoutes.of[F] {
      case POST -> Root / "create" / tag =>
        StockService[F].create(CreateStock(tag)) *> Created()

      case GET -> Root / "findById" / id =>
        Sync[F]
          .catchNonFatal(id.toLong)
          .map(Stock.Id(_))
          .flatMap { id =>
            StockService[F].find(id)
          }
          .flatMap {
            case None        => NotFound()
            case Some(stock) => Ok(stock)
          }
    }
  }
}
