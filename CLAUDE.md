# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Kafka training application: Spring Boot 3 (Java 17) + Spring Batch jobs that periodically generate random-string
messages, publish them to Kafka, and consume them back into MySQL. It exists to demonstrate Kafka concepts
(topics/partitions, keyed producers, `KafkaItemReader`/`KafkaItemWriter`, consumer groups/offsets) through working
code — see the "Guida rapida a Kafka" section of [README.md](README.md) for the concept walkthrough and suggested
exercises.

## Commands

```bash
./mvnw compile              # compile
./mvnw test-compile         # compile tests without running them
./mvnw test                 # run tests
./mvnw test -Dtest=ClassName#methodName   # run a single test
./mvnw spring-boot:run       # run the app (requires Kafka + MySQL reachable, see below)
docker compose up -d         # start Kafka (KRaft, no Zookeeper) + Kafka UI on localhost:8081
```

No Maven install is required — use the wrapper (`mvnw` / `mvnw.cmd`). There is no linter/formatter configured in
this project.

### Running locally

- Kafka comes from `docker-compose.yml` (`localhost:9092`); MySQL is expected to already be running locally (not
  containerized) — connection is via `DB_HOST`/`DB_PORT`/`DB_NAME`/`DB_USER`/`DB_PASSWORD` env vars, defaulting to
  `localhost:3306/kafka_training` with `root`/`root`. Schema is created automatically
  (`spring.jpa.hibernate.ddl-auto=update`, `createDatabaseIfNotExist=true`).
- `spring.batch.job.enabled=false` — jobs never auto-run on startup. They only run via the `@Scheduled` cron
  (`training.schedule.cron`, default every 5 minutes) or via the manual-trigger REST endpoints
  (`POST /api/jobs/{producer1|producer2|consumer}`, see [README.md](README.md)).

## Architecture

Three independent Spring Batch jobs, each package-scoped under `com.training.kafkabasic`:

- **`producer1` / `producer2`** — structurally identical, duplicated rather than shared, each targeting its own
  staging table (`staging_message_1`/`_2`) and Kafka topic (`training-topic-1`/`_2`). Each job has two steps:
  1. **Generate** (tasklet): inserts N random-content rows (`AbstractStagingMessage`, status `PENDING`).
  2. **Publish** (chunk step): reads those rows, maps them to a `TrainingMessage` (JSON payload), sends via
     `KafkaItemWriter`, and flips status to `SENT` via a bulk `@Modifying` query — both writers run inside the same
     `CompositeItemWriter` so publish + status update are transactional together.

  **Important pattern**: the publish step's reader does *not* query `WHERE status = PENDING`. The generate step
  writes the exact IDs it just created into the job's `ExecutionContext`
  (`producer1.generatedIds`/`producer2.generatedIds`), and the publish step's `@StepScope` `ItemReader` late-binds
  those IDs (`#{jobExecutionContext['...']}`) and reads only those rows via `findAllById`. This is deliberate:
  paginating a live `findByStatus(PENDING)` query while the same step mutates rows to `SENT` causes pagination
  drift (rows silently skipped) because each page re-issues the filtered query. Keep this ID-handoff pattern if you
  touch these jobs — reverting to a status-filtered paging reader reintroduces that bug.

- **`consumer`** — one step per topic (`consumeTopic1Step`, `consumeTopic2Step`), each with a `KafkaItemReader`
  built from a manually-assigned partition list (`training.kafka.partitions`, not a subscribing consumer group).
  It drains whatever is currently available on the topic, then finishes — a bounded read matching the periodic
  batch model (`@KafkaListener` would instead stay subscribed forever, see README exercises). Both steps write into
  the single `consumed_message` table, so it holds messages from both producers, distinguished by `producerSource`.

- **`config`** — `KafkaTopicConfig` declares the two topics as `NewTopic` beans (auto-created on startup).
  `KafkaProducerConfig` defines one shared `ProducerFactory<String, TrainingMessage>` (JSON value serializer, no
  type headers) and two `KafkaTemplate` beans (`producer1KafkaTemplate`/`producer2KafkaTemplate`), each with a
  different `defaultTopic` — this split exists because `KafkaItemWriter` sends via `kafkaTemplate.sendDefault(...)`,
  so topic routing is a property of the template, not the writer. The consumer side builds its `Properties`
  (bootstrap servers, group id, `JsonDeserializer` trusted packages) inline in `ConsumerJobConfig` rather than
  through a Spring-managed `ConsumerFactory`, because `KafkaItemReaderBuilder.consumerProperties(...)` takes a raw
  `Properties` object.

- **`common`** — `TrainingMessage` is the Kafka wire format (record: id, source, content, producedAt).
  `AbstractStagingMessage` is a `@MappedSuperclass` shared by `StagingMessage1`/`StagingMessage2` so the two staging
  entities don't duplicate fields despite mapping to different tables.

- **`scheduler.TrainingJobScheduler`** and **`web.JobTriggerController`** both launch the three `Job` beans via
  `JobLauncher`, each run needing a unique `JobParameters` (timestamp) since Spring Batch rejects identical
  parameters for the same job. This launch logic is intentionally duplicated in both places rather than shared,
  since one is a scheduled trigger and the other an on-demand HTTP trigger with a response to build.

### Cross-cutting gotchas worth knowing before editing

- Multiple beans of type `KafkaTemplate<String, TrainingMessage>` exist (one per producer topic) — injection points
  must use `@Qualifier("producer1KafkaTemplate")` / `@Qualifier("producer2KafkaTemplate")`, not just type-based
  autowiring.
- `ItemWriter<T>.write(Chunk<? extends T> chunk)` — lambdas that need to pass `chunk`'s contents to a method
  expecting `Iterable<T>`/`List<T>` (e.g. `repository.saveAll(...)`) hit Java generic-variance errors from the
  `? extends T` wildcard; iterate with a plain for-each loop instead (see `ConsumerJobConfig`'s writers) rather than
  a bare method reference.
