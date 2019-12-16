package com.kubukoz.stock

import com.kubukoz.events._
import fs2.kafka.KafkaProducer
import fs2.kafka.ProducerRecord
import fs2.kafka.ProducerRecords
import cats.data.Chain
import cats.mtl.FunctorTell
import io.circe.syntax._
import cats.tagless.finalAlg
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import fs2.kafka.Serializer
import fs2.kafka.ProducerSettings
import cats.data.Kleisli
import cats.data.OptionT
import cats.mtl.DefaultFunctorTell
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.implicits._
import org.http4s._

object StockApp extends IOApp {
  val serializeEvent: Serializer[IO, StockEvent] = Serializer.string[IO].contramap(_.asJson.noSpaces)

  def run(args: List[String]): IO[ExitCode] = {

    val stockEventProducer = fs2.kafka
      .producerResource[IO]
      .using(
        ProducerSettings(Serializer.unit[IO], serializeEvent).withBootstrapServers("localhost:9092")
      )

    implicit def writeToKleisliOfTell[F[_]: Functor, Log]: FunctorTell[Kleisli[F, FunctorTell[F, Log], ?], Log] =
      new DefaultFunctorTell[Kleisli[F, FunctorTell[F, Log], ?], Log] {
        val functor: Functor[Kleisli[F, FunctorTell[F, Log], ?]] = Functor[Kleisli[F, FunctorTell[F, Log], ?]]

        def tell(l: Log): Kleisli[F, FunctorTell[F, Log], Unit] = Kleisli { _.tell(l) }
      }

    val app: Resource[IO, Unit] = for {
      producer <- stockEventProducer

      service    = StockService.instance[StockEvent.WriteK[IO, ?]]
      routes     = StockRoutes.make(service)
      middleware = WriterSenderMiddleware(sendMessages(producer)) _

      _ <- BlazeServerBuilder[IO].withHttpApp(middleware(routes).orNotFound).resource
    } yield ()

    app.use(_ => IO.never)
  } as ExitCode.Success

  def sendMessages[F[_]: FlatMap](producer: KafkaProducer[F, Unit, StockEvent])(events: Chain[StockEvent]): F[Unit] = {
    val messages = ProducerRecords(events.map(event => ProducerRecord("stock-event", (), event)))

    producer.produce(messages).flatten.void
  }

}

object WriterSenderMiddleware {
  import com.olegpy.meow.effects._

  def apply[F[_]: Sync, Logs: Monoid](send: Logs => F[Unit])(
    routes: HttpRoutes[Kleisli[F, FunctorTell[F, Logs], ?]]
  ): HttpRoutes[F] =
    routes.local[Request[F]](_.mapK(Kleisli.liftK)).mapF { underlying =>
      OptionT.liftF(Ref[F].of(Monoid[Logs].empty)).flatMap { ref =>
        ref.runTell { tell =>
          underlying.mapK(Kleisli.applyK(tell)).map(_.mapK(Kleisli.applyK(tell)))
        } <* OptionT.liftF(ref.get.flatMap(send))
      }
    }
}

final case class CreateStock(tag: String)

@finalAlg
trait StockService[F[_]] {
  def create(stock: CreateStock): F[Unit]
}

object StockService {

  def instance[F[_]: StockEvent.Write]: StockService[F] = new StockService[F] {
    def create(stock: CreateStock): F[Unit] = StockEvent.Write[F].tellOne(StockEvent.Created(stock.tag))
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
