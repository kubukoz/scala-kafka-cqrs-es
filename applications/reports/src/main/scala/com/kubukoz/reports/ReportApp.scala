package com.kubukoz.reports

import com.kubukoz.events.StockEvent
import fs2.kafka._
import fs2.kafka.vulcan._
import fs2.Stream
import scala.concurrent.duration._
import fs2.Chunk
import fs2.Pipe

object ReportApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val avroSettings = AvroSettings(SchemaRegistryClientSettings[IO]("http://localhost:8081"))
    avroSettings.schemaRegistryClient

    val consumer = Resource
      .liftF(avroDeserializer[StockEvent].using(avroSettings).forValue)
      .flatMap { implicit ds =>
        fs2.kafka
          .consumerResource[IO]
          .using(
            ConsumerSettings[IO, Unit, StockEvent]
              .withGroupId("report-app")
              .withBootstrapServers("localhost:9092")
              .withAutoOffsetReset(AutoOffsetReset.Earliest)
              .withIsolationLevel(IsolationLevel.ReadCommitted)
          )
      }
      .evalTap(_.subscribeTo("stock-event"))

    def transactionalProduce[F[_]: ConcurrentEffect: ContextShift, K, V, P](
      settings: TransactionalProducerSettings[F, K, V]
    ): Pipe[F, TransactionalProducerRecords[F, K, V, P], ProducerResult[K, V, P]] =
      records => transactionalProducerStream(settings).flatMap(producer => records.evalMap(producer.produce))

    Stream
      .resource(consumer)
      .flatMap(_.stream)
      .evalMap(handleDecodedEvent)
      .groupWithin(100, 100.millis)
      .map(TransactionalProducerRecords(_))
      .through(
        transactionalProduce(
          TransactionalProducerSettings(
            "report-consumer-stock-event",
            ProducerSettings[IO, Unit, String].withRetries(10).withBootstrapServers("localhost:9092")
          ).withTransactionTimeout(5.seconds)
        )
      )
      .compile
      .drain
  } as ExitCode.Success

  def handleDecodedEvent(
    record: CommittableConsumerRecord[IO, Unit, StockEvent]
  ): IO[CommittableProducerRecords[IO, Unit, String]] = {
    IO(println(record.record.value)) as {
      val records =
        List("foo", "bar", "bazinga").map(ProducerRecord("demo", (), _))

      CommittableProducerRecords(records, record.offset)
    }
  }

}
