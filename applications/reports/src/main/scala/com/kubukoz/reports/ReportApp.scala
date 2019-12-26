package com.kubukoz.reports

import com.kubukoz.events.StockEvent
import fs2.kafka._
import fs2.kafka.vulcan._
import fs2.Stream
import scala.concurrent.duration._
import fs2.Pipe
import cats.tagless.finalAlg
import skunk.Session
import skunk.codec.numeric
import com.kubukoz.events.ReportEvent
import cats.data.Chain
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

object ReportApp extends IOApp {

  implicit val logger = Slf4jLogger.getLogger[IO]

  val avroSettings = AvroSettings(SchemaRegistryClientSettings[IO]("http://localhost:8081"))

  def eventStream(mkSession: Resource[IO, Session[IO]]) =
    Stream
      .eval(avroDeserializer[StockEvent].using(avroSettings).forValue)
      .map { implicit ds =>
        ConsumerSettings[IO, Unit, StockEvent]
          .withGroupId("report-app")
          .withBootstrapServers("localhost:9092")
          .withAutoOffsetReset(AutoOffsetReset.Earliest)
          .withIsolationLevel(IsolationLevel.ReadCommitted)
      }
      .flatMap(consumerStream(_))
      .evalTap(_.subscribeTo("stock-event"))
      .flatMap(_.stream)
      .evalMap(handleDecodedEvent(outTopic = "report-event") { event =>
        mkSession.map(ReportRepository.instance(_)).use { implicit repo =>
          Ref[IO]
            .of(Chain.empty[ReportEvent])
            .flatTap {
              import com.olegpy.meow.effects._
              _.runTell { implicit tell =>
                ReportService.instance[IO].handleStockEvent(event)
              }
            }
            .flatMap(_.get)
        }
      })
      .groupWithin(100, 100.millis)
      .map(TransactionalProducerRecords(_))
      .through {
        val pipeIO = avroSerializer[ReportEvent].using(avroSettings).forValue.map { implicit reportSerializer =>
          transactionalProduce(
            TransactionalProducerSettings(
              "report-consumer-stock-event",
              ProducerSettings[IO, Unit, ReportEvent].withRetries(10).withBootstrapServers("localhost:9092")
            ).withTransactionTimeout(5.seconds)
          )
        }

        stream => Stream.force(pipeIO.map(stream.through))
      }

  import natchez.Trace.Implicits.noop

  def run(args: List[String]): IO[ExitCode] =
    Session.pooled[IO]("localhost", user = "postgres", database = "postgres", max = 10).use { sessionPool =>
      eventStream(sessionPool.flatTap(_.transaction)).compile.drain as ExitCode.Success
    }

  def handleDecodedEvent[F[_]: Functor, G[+_]: Foldable: Functor, K, Event, OutEvent](
    outTopic: String
  )(handler: Event => F[G[OutEvent]])(
    record: CommittableConsumerRecord[F, K, Event]
  ): F[CommittableProducerRecords[F, Unit, OutEvent]] = {
    handler(record.record.value).map { events =>
      val records = events.map(ProducerRecord(outTopic, (), _))
      CommittableProducerRecords(records, record.offset)
    }
  }

  def transactionalProduce[F[_]: ConcurrentEffect: ContextShift, K, V](
    settings: TransactionalProducerSettings[F, K, V]
  ): Pipe[F, TransactionalProducerRecords[F, K, V, Unit], ProducerResult[K, V, Unit]] =
    records => transactionalProducerStream(settings).flatMap(producer => records.evalMap(producer.produce))
}

@finalAlg
trait ReportService[F[_]] {
  def handleStockEvent(event: StockEvent): F[Unit]
}

object ReportService {

  def instance[F[_]: ReportRepository: ReportEvent.Write: Logger: Monad]: ReportService[F] = new ReportService[F] {

    def handleStockEvent(event: StockEvent): F[Unit] =
      ReportRepository[F]
        .createReport(Report()) <* ReportEvent.Write[F].tellOne(ReportEvent.Created("foo")) <* Logger[F].info(
        "Handled event " + event
      )
  }
}

final case class Report()

@finalAlg
trait ReportRepository[F[_]] {
  def createReport(report: Report): F[Unit]
}

object ReportRepository {

  def instance[F[_]: Functor](session: Session[F]): ReportRepository[F] = new ReportRepository[F] {
    import skunk.implicits._

    def createReport(report: Report): F[Unit] = session.execute(sql"select 1".query(numeric.int4)).void
  }
}
