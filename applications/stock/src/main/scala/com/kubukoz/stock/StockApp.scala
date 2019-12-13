package com.kubukoz.stock

import fs2.kafka.KafkaProducer
import fs2.kafka.ProducerRecord
import fs2.kafka.ProducerRecords
import cats.data.Chain
import cats.mtl.FunctorTell
import io.circe.syntax._
import io.circe.generic.extras.Configuration
import io.circe.Codec

object StockApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    import fs2.kafka._

    val result = Ref[IO]
      .of(Chain.empty[StockEvent])
      .flatTap { events =>
        import com.olegpy.meow.effects._

        implicit val addMessages = events.tellInstance

        StockService.instance[IO].create(CreateStock("bar"))
      }
      .flatMap(_.get)
      .map(toKafka)

    val serializeEvent: Serializer[IO, StockEvent] = Serializer.string[IO].contramap(_.asJson.noSpaces)

    producerResource[IO]
      .using(
        ProducerSettings(Serializer.unit[IO], serializeEvent).withBootstrapServers("localhost:9092")
      )
      .use { producer =>
        result.flatMap(producer.produce(_))
      }
  } as ExitCode.Success

  def toKafka(events: Chain[StockEvent]) =
    ProducerRecords(events.map(event => ProducerRecord("stock-event", (), event)))
}

trait StockService[F[_]] {
  def create(stock: CreateStock): F[Unit]
}

object StockService {

  def instance[F[_]: StockEvent.Write]: StockService[F] = new StockService[F] {
    def create(stock: CreateStock): F[Unit] = StockEvent.Write[F].tellOne(StockEvent.Created(stock.tag))
  }
}

final case class CreateStock(tag: String)

object circeConfig {
  implicit val config: Configuration = Configuration.default.withDiscriminator("@type")
}

sealed trait StockEvent extends Product with Serializable

object StockEvent {
  final case class Created(tag: String) extends StockEvent

  import circeConfig._
  import io.circe.generic.extras.semiauto._

  implicit val codec: Codec[StockEvent] = deriveConfiguredCodec

  type Write[F[_]] = FunctorTell[F, Chain[StockEvent]]

  def Write[F[_]](implicit F: Write[F]): Write[F] = F

  implicit class WriteOne[F[_], G[_], V](tell: FunctorTell[F, G[V]]) {
    def tellOne(value: V)(implicit G: Applicative[G]): F[Unit] = tell.tell(G.pure(value))
  }
}
