# kafka-basic

Applicazione di training per **Apache Kafka**, costruita con **Spring Boot 3** (Java 17) e **Spring Batch**. Due job "producer" generano stringhe casuali, le salvano su MySQL e le pubblicano su Kafka; un job "consumer" legge dai topic e persiste ciò che riceve. Tutti e tre i job girano periodicamente (ogni 5 minuti di default) e possono anche essere lanciati a mano.

## Architettura

```
                 ┌────────────────────┐        ┌──────────────────────┐
  ogni 5 min ──▶ │  producer1Job       │        │  producer2Job        │
                 │  1. genera N righe  │        │  1. genera N righe   │
                 │     in staging_1    │        │     in staging_2     │
                 │  2. pubblica su     │        │  2. pubblica su      │
                 │     training-topic-1│        │     training-topic-2 │
                 │  3. marca SENT      │        │  3. marca SENT       │
                 └─────────┬───────────┘        └──────────┬───────────┘
                           │                                │
                           ▼                                ▼
                  ┌───────────────────────────────────────────────┐
                  │              Apache Kafka (Docker)             │
                  │   training-topic-1          training-topic-2   │
                  └───────────────────┬─────────────────────────────┘
                                       │
                                       ▼
                        ┌───────────────────────────┐
        ogni 5 min ──▶  │       consumerJob         │
                        │  step 1: legge topic 1     │
                        │  step 2: legge topic 2     │
                        │  scrive in consumed_message│
                        └───────────────────────────┘
```

Tabelle MySQL create automaticamente (`ddl-auto=update`): `staging_message_1`, `staging_message_2`, `consumed_message`, oltre alle tabelle interne di Spring Batch (`BATCH_JOB_*`, `BATCH_STEP_*`).

## Prerequisiti

- Java 17
- Docker Desktop (per Kafka — il progetto include il Maven Wrapper, non serve installare Maven)
- Un'istanza MySQL locale già in esecuzione (l'app crea automaticamente lo schema/le tabelle, ma non il database server)

> **Windows + WSL**: se Docker e/o MySQL sono installati solo dentro WSL (non Docker Desktop con
> integrazione visibile dal terminale Windows/PowerShell/Git Bash che stai usando), i comandi
> `docker`/`mysql` lanciati direttamente da Windows falliscono con "command not found" anche se
> funzionano perfettamente dentro la distro. In quel caso lanciali con
> `wsl.exe -- bash -lc "<comando>"`, oppure apri direttamente un terminale WSL.

## 1. Avviare Kafka

```bash
docker compose up -d
```

Questo avvia:
- **Kafka** (modalità KRaft, senza Zookeeper) su `localhost:9092`
- **Kafka UI** su [http://localhost:8081](http://localhost:8081) — utile per vedere topic, partizioni, messaggi e consumer group da browser

## 2. Configurare MySQL

Crea (se non esiste già) un utente/database che l'app possa usare, oppure lascia che l'app crei da sola il database `kafka_training` (grazie a `createDatabaseIfNotExist=true`) purché l'utente configurato abbia i permessi.

L'app legge la configurazione da variabili d'ambiente (default fra parentesi):

| Variabile | Default |
|---|---|
| `DB_HOST` | `localhost` |
| `DB_PORT` | `3306` |
| `DB_NAME` | `kafka_training` |
| `DB_USER` | `root` |
| `DB_PASSWORD` | `root` |

Esempio:

```bash
export DB_USER=root
export DB_PASSWORD=la-tua-password
```

(su Windows/PowerShell: `$env:DB_PASSWORD = "la-tua-password"`)

## 3. Avviare l'applicazione

```bash
./mvnw spring-boot:run
```

All'avvio **nessun job parte automaticamente** (`spring.batch.job.enabled=false`): partono solo tramite lo scheduler (ogni 5 minuti, configurabile con `training.schedule.cron`) o tramite gli endpoint REST.

## Endpoint REST utili per sperimentare

| Metodo | Path | Effetto |
|---|---|---|
| `POST` | `/api/jobs/producer1` | Lancia subito il job producer1 |
| `POST` | `/api/jobs/producer2` | Lancia subito il job producer2 |
| `POST` | `/api/jobs/consumer` | Lancia subito il job consumer |
| `GET` | `/api/messages/staging` | Conteggio righe PENDING/SENT nelle due tabelle di staging |
| `GET` | `/api/messages/consumed` | Ultimi 20 messaggi consumati da Kafka |

Esempio di sessione di prova:

```bash
curl -X POST http://localhost:8080/api/jobs/producer1
curl -X POST http://localhost:8080/api/jobs/producer2
curl -X POST http://localhost:8080/api/jobs/consumer
curl http://localhost:8080/api/messages/consumed
```

---

## Guida rapida a Kafka (concetti usati in questo progetto)

**Topic e partizioni** — un topic (`training-topic-1`, `training-topic-2`) è diviso in più partizioni (qui 3, vedi `training.kafka.partitions`); ogni partizione è un log ordinato e immutabile. Le partizioni permettono parallelismo: più consumer nello stesso *consumer group* possono leggere partizioni diverse in parallelo.

**Chiave di partizionamento** — ogni `KafkaItemWriter` usa l'id del messaggio come chiave (`itemKeyMapper`). Kafka instrada i messaggi con la stessa chiave sempre sulla stessa partizione, garantendo l'ordine *per chiave*. Prova a inviare più messaggi e osserva su quale partizione finiscono nella Kafka UI.

**Producer** — configurato in [`KafkaProducerConfig`](src/main/java/com/training/kafkabasic/config/KafkaProducerConfig.java): chiave serializzata come stringa, valore come JSON (`JsonSerializer`). I messaggi viaggiano come `TrainingMessage { id, source, content, producedAt }`.

**Consumer e consumer group** — il job consumer usa `KafkaItemReader`, la classe che Spring Batch fornisce per leggere da Kafka dentro uno step chunk-oriented. A differenza di un `@KafkaListener` (sempre in ascolto), `KafkaItemReader` **assegna manualmente le partizioni** e legge tutto ciò che è disponibile in quel momento, poi si ferma — comportamento perfetto per un job schedulato che gira ogni N minuti. Il `group.id` (`training-consumer`, in `application.yml`) è comunque usato per salvare gli offset committati.

**Offset e "at-least-once"** — Kafka non cancella i messaggi dopo la lettura: ogni consumer group tiene traccia di un *offset* (posizione) per partizione. Se il job fallisce dopo aver letto ma prima di scrivere su MySQL, alla riesecuzione potrebbe rileggere gli stessi messaggi (consegna *at-least-once*): è il motivo per cui in produzione si progettano scritture idempotenti.

**Ispezionare i topic**

- Via browser: [http://localhost:8081](http://localhost:8081) (Kafka UI)
- Via CLI, entrando nel container:
  ```bash
  docker exec -it kafka-basic-broker /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic training-topic-1 --from-beginning
  ```
- Elenco topic:
  ```bash
  docker exec -it kafka-basic-broker /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list
  ```

### Esercizi suggeriti per approfondire

1. **Cambia il numero di partizioni** (`training.kafka.partitions`) e osserva come cambia la distribuzione dei messaggi nella Kafka UI.
2. **Aggiungi un secondo consumer group**: duplica `ConsumerJobConfig` con un `group.id` diverso e verifica che entrambi i gruppi ricevano tutti i messaggi (a differenza di due consumer nello *stesso* gruppo, che si dividono le partizioni).
3. **Introduci un errore volontario** nel processor del consumer (es. lancia un'eccezione per un messaggio su tre) e osserva il comportamento del job/step (retry, fallimento) — ottimo modo per capire la gestione degli errori di Spring Batch.
4. **Aggiungi un topic di dead-letter**: cattura i messaggi che falliscono ripetutamente e ripubblicali su `training-topic-dlq`.
5. **Sostituisci `KafkaItemReader` con un `@KafkaListener`** per vedere la differenza fra un consumer "sempre attivo" e uno "a lettura periodica".
6. **Scrivi un test con Testcontainers** (`kafka` + `mysql` module) per verificare l'intero flusso end-to-end senza dipendere da Docker Compose manuale.

## Configurazione principale (`application.yml`)

| Proprietà | Significato |
|---|---|
| `training.kafka.topic-1` / `topic-2` | Nomi dei due topic |
| `training.kafka.partitions` | Partizioni create per ciascun topic |
| `training.producer.messages-per-run` | Quante righe genera ogni producer ad ogni esecuzione |
| `training.schedule.cron` | Espressione cron di Spring per lo scheduling dei job (default: ogni 5 minuti) |
