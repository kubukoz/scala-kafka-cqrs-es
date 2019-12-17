# scala-kafka-cqrs-es

[![License](http://img.shields.io/:license-Apache%202-green.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

This is a demo application showing a potential use-case for Apache Kafka with at-least-once, transactional message processing.

## Architecture

The following applications are available:

- stock service
- report service
- notification service

### Stock Service

- provides HTTP endpoints for managing stock
- stores stock state in its database
- sends a `StockEvent.Created` to `stock-event` topic - smuggled through the database to ensure at-least-once delivery
- run a concurrent process to send events pending in the database

### Report Service

- reads `StockEvent`s
- builds its own read model of stock for reports
- builds reports
- sends `ReportEvent.Created` to `report-event` topic
- serves reports over HTTP

### Notification Service

- consumes `ReportEvent`s
- notifies customers that a new report is available (in the demo case, that'll be just printing to logs)
- keeps track of the amount of notifications sent
- serves stats (notifications sent) over HTTP
