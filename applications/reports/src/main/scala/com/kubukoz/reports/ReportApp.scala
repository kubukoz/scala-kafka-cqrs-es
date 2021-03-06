package com.kubukoz.reports

import com.kubukoz.events.StockEvent
import fs2.kafka._
import fs2.kafka.vulcan._
import fs2.Stream
import scala.concurrent.duration._
import fs2.Pipe

object ReportApp extends IOApp {

  val avroSettings = AvroSettings(SchemaRegistryClientSettings[IO]("http://localhost:8081"))

  def run(args: List[String]): IO[ExitCode] =
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
      .evalMap(handleDecodedEvent(outTopic = "demo")(handler))
      .groupWithin(100, 100.millis)
      .map(TransactionalProducerRecords(_))
      .through {
        transactionalProduce(
          TransactionalProducerSettings(
            "report-consumer-stock-event",
            ProducerSettings[IO, Unit, String].withRetries(10).withBootstrapServers("localhost:9092")
          ).withTransactionTimeout(5.seconds)
        )
      }
      .compile
      .drain as ExitCode.Success

  // Commit DB transaction here for at least once processing.
  def handler(event: StockEvent): IO[List[String]] =
    IO(println(event)) *>
      IO.pure(List("foo", "bar", "bazinga", event.toString))

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

  def transactionalProduce[F[_]: ConcurrentEffect: ContextShift, K, V, P](
    settings: TransactionalProducerSettings[F, K, V]
  ): Pipe[F, TransactionalProducerRecords[F, K, V, P], ProducerResult[K, V, P]] =
    records => transactionalProducerStream(settings).flatMap(producer => records.evalMap(producer.produce))
}
