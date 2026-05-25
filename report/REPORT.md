# Secondo Progetto — Corso di Big Data

**[TODO autori e info di consegna]**
- Studente/i: _Massimo D'Alessandro, [collega]_
- Matricole: _[matricole]_
- Corso: Big Data, Prof. Riccardo Torlone
- A.A.: 2025/2026
- Data di consegna: 04/06/2026
- Repository: **[TODO: link GitHub]**

---

## 1. Introduzione

Il progetto sperimenta l'uso comparativo di quattro tecnologie per l'analisi di big data sul *Flight Delay Dataset 2024* (Kaggle, ~7 milioni di record, 35 colonne). L'obiettivo non è soltanto realizzare le analisi richieste, ma confrontare i framework dal punto di vista di **espressività**, **semplicità implementativa**, **efficienza** e **scalabilità**, sia su una singola macchina che su un cluster distribuito.

Le tecnologie selezionate sono tutte e quattro quelle previste dalla traccia: **MapReduce (Hadoop Streaming)**, **HiveQL su Tez**, **Spark Core (RDD)** e **Spark SQL (DataFrame)**. Per l'analisi 3.1 (statistiche delle compagnie aeree) sono state realizzate tutte e quattro le implementazioni; per l'analisi **[TODO 2° job — 3.2 o 3.3]** sono state realizzate **[TODO N implementazioni — coordinare col collega]**.

L'analisi sperimentale è stata condotta in due ambienti: in **locale** su Mac Apple Silicon (Hadoop pseudo-distribuito, Spark `local[*]`) e su un **cluster EMR su AWS** (1 master + 4 core, istanze `m5.xlarge`). Sono state usate quattro dimensioni di input (50%, 100%, 200%, 400% del dataset originale ripulito) per misurare la scalabilità.

---

## 2. Tecnologie utilizzate

Le versioni in uso, sia in locale che sul cluster, sono riassunte nella Tabella 1.

**Tabella 1 — Versioni dei framework.**

| Componente | Locale | EMR (release `emr-7.2.0`) |
|---|---|---|
| Hadoop | 3.4.3 | 3.4.0 |
| Hive | 4.0.1 | 3.1.3 |
| Tez | 0.10.4 | 0.10.3 |
| Spark | 3.5.8 | 3.5.0 |
| JDK | Temurin 11 | Corretto 8/17 (default EMR) |
| Python | 3.11 (per PySpark) | 3.9 (EMR default) |

Su entrambi gli ambienti i framework convivono sullo stesso filesystem (HDFS in locale, S3 + HDFS interno su EMR) ma usano strategie di compute diverse:
- **MapReduce e HiveQL** sono eseguiti sotto YARN come container distinti, con shuffle che passa per disco/rete.
- **Spark in locale** gira in modalità `local[*]` (singolo processo con N thread, N = numero di core fisici). Su EMR Spark gira come applicazione YARN come gli altri.

Questa asimmetria — Spark locale single-process vs MR/Hive multi-container — è uno dei punti del commento critico (Sezione 6.3).

---

## 3. Preparazione dei dati

Il dataset originale è un singolo CSV di **7.079.081 record** e **35 colonne**, di dimensione 1.2 GB. La sezione 4 della traccia richiede operazioni di preparazione: si è scelto di implementarle in un **unico stage Spark** (`prep/prepare.py`) che produce il dataset *clean* da cui partono tutti i job successivi.

### 3.1 Operazioni effettuate

1. **Selezione delle colonne rilevanti.** Di 35 colonne, solo 12 sono effettivamente usate da almeno una delle tre analisi previste. Sono state mantenute soltanto queste:

   `op_unique_carrier, origin, month, dep_delay, arr_delay, cancelled, cancellation_code, carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay`

   Riduzione dimensionale: **65% in meno** di dimensione su disco (da 1.2 GB a ~220 MB), con beneficio significativo sul tempo di I/O dei job a valle.

2. **Rimozione dei record privi di chiavi.** Sono stati eliminati i record con `op_unique_carrier` o `origin` nulli o vuoti: senza queste colonne il record non è utilizzabile in nessuna delle tre analisi. Il filtro ha eliminato **0 record** sul dataset BTS (le colonne chiave sono sempre valorizzate), ma il filtro resta nella pipeline per protezione semantica.

3. **Rimozione dell'header.** Il CSV originale ha un header sulla prima riga. Lo stage di prep lo rimuove, producendo un CSV *header-less* a 12 colonne. Questo evita due classi di problemi nei job:
   - per **MapReduce** l'header può essere visto solo dal mapper sul primo split: con dataset multi-block (>128 MB) la rilevazione automatica fallisce;
   - per gli altri framework elimina la fragilità del "skip header" e permette di lavorare con uno schema fisso noto a priori.

4. **Nessuna trasformazione di tipo.** Il file rimane in formato CSV testo, con tutte le colonne come stringhe. Il cast a tipo numerico è demandato a ciascun job in base alle sue necessità (vedere ad esempio HiveQL, Sezione 4.2.2). Questa scelta è intenzionale: rende il dataset *clean* uniforme tra i quattro framework, evitando vantaggi indebiti dei framework che leggono direttamente formati colonnari tipizzati (Parquet/ORC).

### 3.2 Strategia di replicazione del dataset

Per studiare la scalabilità sono state generate quattro versioni del dataset clean, a partire dal file prodotto dalla prep (`prep/replicate.py`):

**Tabella 2 — Dimensioni dei dataset di bench.**

| Tag | Tecnica | Record | Dimensione |
|---|---|---|---|
| `bench_50` | sub-sampling random (seed=42), Spark `.sample(0.5)` | ~3.5 M | 110 MB |
| `bench_100` | copia di `clean` | 7.1 M | 221 MB |
| `bench_200` | concatenazione 2× di `clean` (replica naive) | 14.2 M | 442 MB |
| `bench_400` | concatenazione 4× di `clean` (replica naive) | 28.3 M | 883 MB |

Per le dimensioni minori si è usato il sub-sampling random (preserva le distribuzioni delle chiavi); per quelle maggiori la concatenazione naive del file con se stesso. La replica naive **raddoppia/quadruplica i `numero_voli`** nell'output di 3.1, ma lascia identiche le aggregazioni statistiche derivate (MIN/MAX/AVG/COLLECT_SET), per costruzione. La scelta è motivata dall'esigenza di **misurare la scalabilità di lettura + shuffle** del framework sul volume crescente senza alterare la struttura semantica del problema.

---

## 4. Job 1 — Statistiche delle compagnie aeree (analisi 3.1)

### 4.1 Specifica

Per ciascuna compagnia aerea presente nel dataset, produrre un record contenente:
- il codice della compagnia (`op_unique_carrier`);
- una lista di **aeroporti di partenza** servita dalla compagnia. Per ciascun aeroporto:
  - numero di voli operati,
  - ritardo minimo, massimo e medio in arrivo,
  - tasso di cancellazione dei voli,
  - elenco dei mesi in cui la compagnia opera su quell'aeroporto.

È stata scelta la chiave **aeroporto di partenza** (`origin`) anziché la **tratta completa** (`origin`, `dest`): la traccia ammette esplicitamente entrambe (*"aeroporti di partenza O tratte servite"*). Origin-only mantiene il numero di record di output ridotto (~1500 coppie carrier×origin) e produce un commento dei risultati più immediato.

### 4.2 Algoritmo

L'algoritmo è semplice e identico in tutte le implementazioni. È composto da due fasi logiche:

```
Stage 1 (aggregazione per tratta):
    GROUP BY (op_unique_carrier, origin)
    → per ogni gruppo: COUNT, MIN/MAX/AVG su arr_delay,
      AVG su cancelled, SET-collezione di month

Stage 2 (annidamento per compagnia):
    GROUP BY (op_unique_carrier)
    → per ogni gruppo: lista di struct contenente
      origin + statistiche calcolate allo Stage 1
```

L'output finale è un record per compagnia, con un campo annidato `report_tratte` che è un array di struct.

### 4.3 Implementazioni

#### 4.3.1 MapReduce (Hadoop Streaming)

Implementato come **due job MR concatenati**, con mapper e reducer scritti in Python (Hadoop Streaming). Le scelte chiave sono:

- **Chiave composta nello Stage 1.** Il mapper emette `(op_unique_carrier, origin) <tab> <payload-json>`. Per fare in modo che Hadoop partizioni e ordini sulla coppia, e non solo sul primo campo, vengono impostate due opzioni Streaming:
   ```
   -D stream.num.map.output.key.fields=2
   -D mapreduce.map.output.key.field.separator=\t
   ```
- **In-mapper combiner.** Il mapper accumula i record in un `dict` Python locale e emette il risultato aggregato solo a fine input. Su 7M righe di input ciò produce ~1500 righe in output (15 carrier × ~100 aeroporti unici), riducendo enormemente il volume di shuffle.
- **Output finale come JSON-line.** Il reducer dello Stage 2 emette una riga JSON per compagnia, formato identico a quello prodotto dagli altri tre framework. Permette il confronto cross-tecnologia con un semplice `diff`.

Lo pseudocodice del mapper di Stage 1 è riportato di seguito (versione semplificata):

```python
acc = {}
for line in stdin:
    # Hadoop Streaming prepone l'offset del record come chiave:
    # "<offset>\t<value>\n". Lo strippo se presente.
    line = strip_streaming_offset(line)
    row = csv_parse(line)
    carrier, origin = row[COL_CARRIER], row[COL_ORIGIN]
    if not carrier or not origin:
        continue
    key = (carrier, origin)
    acc[key] = merge(acc.get(key), record_stats(row))

for (carrier, origin), stats in acc.items():
    print(f"{carrier}\t{origin}\t{json.dumps(stats)}")
```

#### 4.3.2 HiveQL su Tez

Implementato in HiveQL come **singola query annidata** (sub-query per il primo GROUP BY, GROUP BY esterno per l'annidamento), che genera un **unico DAG Tez** con pipelining tra i due vertex. Una versione iniziale CTAS-based (con tabelle intermedie materializzate per stage) è stata abbandonata dopo aver osservato un'inefficienza del 34% sul size 400% — vedere Sezione 6.2.

Le scelte implementative principali sono:

- **Tabella esterna sui CSV** con `OpenCSVSerde` e schema dichiarato a 12 colonne. La tabella registra solo lo schema nel metastore Hive, i CSV restano dove sono su HDFS/S3.
- **CAST espliciti.** `OpenCSVSerde` di Hive tratta tutte le colonne come `STRING` ignorando i tipi dichiarati. Senza cast esplicito, `MIN/MAX` su `arr_delay` farebbe confronti lessicografici (es. `"-16.0" > "-20.0"` perché ordinati come stringhe). Si è dovuto introdurre `CAST(arr_delay AS DOUBLE)` su ogni occorrenza.
- **Output via JsonSerDe.** L'output finale è scritto come JSON-line tramite il `JsonSerDe` di HCatalog. Per propagare correttamente i nomi delle colonne (l'`INSERT OVERWRITE DIRECTORY` non lo fa con il SerDe), si è usata una tabella esterna di destinazione con schema esplicito e `INSERT OVERWRITE TABLE`.

Frammento della query:

```sql
INSERT OVERWRITE TABLE report_voli_per_carrier_json
SELECT op_unique_carrier,
       collect_list(named_struct(
           'origin', origin,
           'numero_voli', numero_voli,
           'ritardo_minimo_arrivo', ritardo_minimo_arrivo,
           ... -- altre statistiche
       )) AS report_tratte
FROM (
    SELECT op_unique_carrier, origin,
           COUNT(*) AS numero_voli,
           MIN(CAST(arr_delay AS DOUBLE)) AS ritardo_minimo_arrivo,
           MAX(CAST(arr_delay AS DOUBLE)) AS ritardo_massimo_arrivo,
           AVG(CAST(arr_delay AS DOUBLE)) AS ritardo_medio_arrivo,
           AVG(CAST(cancelled AS DOUBLE))  AS tasso_cancellazioni,
           sort_array(collect_set(CAST(month AS INT))) AS mesi_operativi
    FROM flight_raw
    GROUP BY op_unique_carrier, origin
) AS stats
GROUP BY op_unique_carrier;
```

#### 4.3.3 Spark Core (API RDD)

Implementato in PySpark usando soltanto l'API RDD (no DataFrame). Le scelte chiave:

- **`reduceByKey` per Stage 1.** Combina parziali con la stessa chiave già lato mapper, equivalente all'in-mapper combiner del MR custom.
- **`groupByKey + mapValues(list)` per Stage 2.** Raccoglie le tratte di ogni carrier in una lista. Si è scelto `groupByKey` per chiarezza didattica; in produzione `aggregateByKey` sarebbe leggermente più efficiente, ma su questa dimensione di output (<2k record) la differenza è trascurabile.
- **Header già rimosso a monte.** Grazie alla pipeline di prep, il driver non deve gestire il caso "header sul primo split": lo schema è fisso e gli indici delle colonne sono `hardcoded` nel codice.

```python
parsed = (lines
    .filter(lambda l: l)
    .map(parse_csv_line)
    .map(extract_record)
    .filter(lambda x: x is not None))

stats = parsed.reduceByKey(merge_stats)

report = (stats
    .map(lambda kv: (kv[0][0], stat_to_dict(kv[0][1], kv[1])))
    .groupByKey()
    .mapValues(list)
    .map(lambda kv: {"op_unique_carrier": kv[0], "report_tratte": kv[1]}))

report.map(json.dumps).saveAsTextFile(OUTPUT_PATH)
```

#### 4.3.4 Spark SQL (DataFrame)

Implementato come due `groupBy().agg()` consecutivi su DataFrame:

- **Schema esplicito.** Niente `inferSchema` (richiederebbe uno scan iniziale): lo schema delle 12 colonne è dichiarato con `StructType` direttamente nel codice, coerente con quello prodotto dalla prep.
- **`collect_set` per i mesi e `collect_list(struct(...))` per l'annidamento.** Funzioni di aggregazione di Catalyst, ottimizzate.
- **`write.json()` finale.** Spark SQL scrive il DataFrame come JSON-line con AQE che gestisce il numero di partizioni (collassa a 1 file per dataset piccolo).

```python
schema = StructType([...])  # 12 colonne tipizzate
df = spark.read.schema(schema).csv(INPUT_PATH, header=False)

stats = df.groupBy("op_unique_carrier", "origin").agg(
    F.count("*").alias("numero_voli"),
    F.min("arr_delay").alias("ritardo_minimo_arrivo"),
    F.max("arr_delay").alias("ritardo_massimo_arrivo"),
    F.avg("arr_delay").alias("ritardo_medio_arrivo"),
    F.avg("cancelled").alias("tasso_cancellazioni"),
    F.collect_set("month").alias("mesi_operativi"))

result = stats.groupBy("op_unique_carrier").agg(
    F.collect_list(F.struct(...)).alias("report_tratte"))

result.write.mode("overwrite").json(OUTPUT_PATH)
```

### 4.4 Equivalenza dei risultati

I quattro framework producono lo **stesso insieme di risultati** sui rispettivi output JSON-line. La verifica è stata effettuata su un carrier campione (`9E`), che ha **64 tratte sul `bench_50`** e **123 tratte sul `bench_100`** in tutti e quattro gli output, con valori statistici identici (i valori derivati come MIN/MAX/AVG coincidono esattamente). L'ordine dei record nei file di output varia tra framework: MapReduce e HiveQL producono output ordinato per chiave hash (alfabeticamente nel nostro caso), Spark Core e Spark SQL no.

### 4.5 Prime 10 righe dei risultati

Le prime 10 righe dell'output di ciascun framework sono incluse nel repository in `report/first_job/snippets/<framework>_<size>.json`. Esempio dal file `mapreduce_100.json` (un record per carrier `9E`, formato JSON-line, qui *pretty-printed* e tratte limitate alle prime 5 per ragioni di spazio):

```json
{
  "op_unique_carrier": "9E",
  "report_tratte": [
    {"origin":"ABE", "numero_voli":4,  "ritardo_minimo_arrivo":-26.0, "ritardo_massimo_arrivo":-12.0, "ritardo_medio_arrivo":-18.5, "tasso_cancellazioni":0.0,    "mesi_operativi":[6,7,9]},
    {"origin":"ALB", "numero_voli":4,  "ritardo_minimo_arrivo":-16.0, "ritardo_massimo_arrivo":191.0, "ritardo_medio_arrivo":53.25, "tasso_cancellazioni":0.0,    "mesi_operativi":[3,6,8]},
    {"origin":"ATL", "numero_voli":62, "ritardo_minimo_arrivo":-25.0, "ritardo_massimo_arrivo":80.0,  "ritardo_medio_arrivo":-3.31, "tasso_cancellazioni":0.016, "mesi_operativi":[1,2,3,4,5,7,8,9,10,11,12]},
    {"origin":"AUS", "numero_voli":8,  "ritardo_minimo_arrivo":-22.0, "ritardo_massimo_arrivo":50.0,  "ritardo_medio_arrivo":3.50,  "tasso_cancellazioni":0.0,    "mesi_operativi":[3,4,5,6]},
    {"origin":"AVL", "numero_voli":2,  "ritardo_minimo_arrivo":-28.0, "ritardo_massimo_arrivo":-15.0, "ritardo_medio_arrivo":-21.5, "tasso_cancellazioni":0.0,    "mesi_operativi":[11]}
  ]
}
```

L'output completo a 100% contiene **15 record** (uno per carrier), ognuno con `report_tratte` di lunghezza variabile (tra 64 e ~330 tratte). I 15 carrier osservati nei dati BTS 2024 sono: `9E, AA, AS, B6, DL, F9, G4, HA, MQ, NK, OH, OO, UA, WN, YX`.

---

## 5. Job 2 — **[TODO da completare con il collega]**

_Sezione da aggiungere quando il secondo job sarà completato. Manterrà la stessa struttura del Job 1 (specifica → algoritmo → 4 implementazioni → prime 10 righe)._

---

## 6. Analisi sperimentale

### 6.1 Setup di prova

Due ambienti di esecuzione, riassunti in Tabella 3.

**Tabella 3 — Ambienti di prova.**

| Caratteristica | Locale | EMR |
|---|---|---|
| Hardware | MacBook Apple Silicon (8 core, 16 GB RAM) | 1 master + 4 core, `m5.xlarge` (4 vCPU, 16 GB RAM ognuna) |
| Sistema operativo | macOS 26.3 | Amazon Linux 2 |
| HDFS | pseudo-distribuito (1 NameNode + 1 DataNode loopback) | distribuito (5 DataNode sui 5 nodi) |
| Cluster manager | YARN (1 NodeManager) | YARN (5 NodeManager) |
| Spark | `local[*]` (1 processo, 8 thread) | `--master yarn`, deploy `cluster` |
| Filesystem dati | HDFS locale (loopback) | **S3** (`s3a://`) per input/output, HDFS interno per i tarball Tez/Spark |

L'asimmetria principale è che **in locale Spark non passa per YARN**: i task girano come thread nello stesso processo. Questo è il modo "naturale" di usare Spark per sviluppo/test su una singola macchina, ma ha conseguenze sui tempi misurati: Spark in locale evita per costruzione l'overhead di startup container, scheduling YARN e shuffle via rete. Su EMR tutti e quattro i framework girano sotto YARN in modo uniforme.

### 6.2 Strategia di esecuzione e raccolta tempi

Ogni esecuzione misura il **wall-clock** completo del job (compresi avvio JVM, lettura input, scrittura output) tramite `date +%s` prima/dopo l'invocazione. Per ridurre il rumore della prima invocazione (che paga sempre overhead di JVM cold-start, localization Tez tarball, sessione metastore), in locale ogni configurazione `(framework, size)` è stata eseguita **3 volte**, scartando la prima run e mediando le due successive. Su EMR, per contenere il costo del cluster, ogni configurazione è stata eseguita **una volta**: i tempi cluster vanno letti come ordine di grandezza più che come misure precise.

L'orchestrazione è automatizzata: `bench/run_all.sh` per il locale, sottomissione di step EMR via `aws emr add-steps` per il cluster. I tempi sono raccolti in `metrics/first_job/results.csv` (locale) e `metrics/first_job_emr/results.csv` (cluster).

### 6.3 Risultati locali

**Tabella 4 — Tempi medi locali (secondi), 3 run con scarto della prima.**

| Framework | 50% | 100% | 200% | 400% | Scaling 50→400 |
|---|---|---|---|---|---|
| MapReduce | 43.0 | 48.0 | 58.5 | 76.0 | **1.77×** |
| HiveQL (CTAS, 3 stage) | 37.0 | 49.5 | 84.0 | 132.0 | 3.57× |
| HiveQL (pipelined, 1 stage) | 38.5 | 45.5 | 56.5 | 87.5 | 2.27× |
| Spark Core | 14.0 | 21.0 | 32.5 | 62.5 | **4.46×** |
| Spark SQL | 13.5 | 16.0 | 26.0 | 45.5 | 3.37× |

Tre osservazioni principali. **Primo**, MapReduce ha il fattore di scaling più basso (1.77×) ma anche il tempo assoluto più alto sulla size piccola — segno che il job è **dominato dallo startup-overhead** di YARN (avvio container, localization). **Secondo**, Spark Core ha il fattore di scaling più alto (4.46×) ma è il framework "puro" — senza optimizer — il cui tempo cresce quasi linearmente con il volume di dati. **Terzo**, Spark SQL è in assoluto il più veloce a tutte le dimensioni, beneficio di Catalyst optimizer + AQE.

Il caso di HiveQL merita una nota a parte: la versione **CTAS-based** (tre statement SQL distinti, due `CREATE TABLE AS` intermedie + un `INSERT OVERWRITE`) è 34% più lenta della versione **pipelined** (un'unica query con subquery) sul size 400%. La spiegazione tecnica è discussa in 7.2.

I plot di scalabilità sono in `metrics/first_job/plots/walltime_by_size.png` e `walltime_bars.png`.

### 6.4 Risultati su cluster EMR

**Tabella 5 — Tempi su EMR (1 master + 4 core `m5.xlarge`, una run).**

| Framework | 50% | 100% | 200% | 400% | Scaling 50→400 |
|---|---|---|---|---|---|
| MapReduce | 110.4 | 114.2 | 120.1 | 150.2 | **1.36×** |
| HiveQL pipelined | 52.1 | 60.1 | 68.1 | 78.1 | 1.50× |
| Spark Core | 42.1 | 44.1 | 52.1 | 70.1 | 1.67× |
| Spark SQL | 38.1 | 38.1 | 48.1 | 48.1 | **1.26×** |

I fattori di scaling **crollano** rispetto al locale: Spark SQL passa da 3.37× a 1.26×, Spark Core da 4.46× a 1.67×. Questa è la **firma del parallelismo distribuito**: i 4 worker assorbono il volume crescente in parallelo. Su Spark SQL, in particolare, il tempo dei size 100% e 200% è identico (38s) e poi sale solo a 48s sul 400% — segno che la macchina è "sovra-dimensionata" rispetto al carico (il dataset entra in memoria, lo shuffle è trascurabile, il bottleneck si sposta sull'I/O da S3 che è quasi costante).

**Letture importanti.**

- **HiveQL su EMR sul size 400% impiega meno tempo che in locale** (78s vs 87.5s, −11%). È l'unico framework che migliora in cluster. La spiegazione: la versione pipelined ha due GROUP BY in pipeline che traggono pieno beneficio dei 4 worker paralleli, mentre l'overhead di setup di Tez si ammortizza sui dati più grandi.

- **MapReduce su EMR raddoppia il tempo rispetto al locale** (150s vs 76s sul 400%, +97%). Il motivo è la frammentazione del job in **due step EMR distinti** (stage1 + stage2), ognuno con il proprio startup container, gather-results e scrittura intermedia su S3. Su EMR i due stage non condividono container (uno step EMR = uno o più job MR/Spark/Hive **isolati**); in locale i due `hadoop jar` consecutivi nello stesso `run.sh` possono almeno riutilizzare la JVM del client. Questo penalizza fortemente l'approccio multi-stage di MR.

### 6.5 Confronto locale ↔ cluster

Il grafico `metrics/first_job/plots/compare_local_vs_emr.png` mostra side-by-side le due curve di scalabilità. Il rapporto EMR/Locale (`compare_speedup.png`) evidenzia tre comportamenti distinti:

1. **MapReduce**: rapporto sempre > 1, fino a 2.6× sul size 50%. EMR è una pena per MR quando il dataset è piccolo, e resta peggiore anche sul 400%. **MR non beneficia del cluster su questa workload**.
2. **Spark Core/SQL**: rapporto ~1.0–1.5×. Su size piccola il cluster è leggermente più lento (overhead YARN), su size 400% diventa concorrenziale o leggermente migliore. **Il vantaggio del cluster si manifesta solo sui dataset più grandi**.
3. **HiveQL pipelined**: rapporto > 1 sotto i 200%, <1 a 400%. **Hive si avvantaggia chiaramente del cluster sui volumi maggiori**.

Il gap tra Spark SQL e HiveQL pipelined si riduce passando dal locale al cluster: da **1.92×** (locale, 87.5 / 45.5) a **1.62×** (EMR, 78.1 / 48.1). Su dataset ancora più grandi (TB scale) le due curve potrebbero convergere — la tesi "Hive pipelined è competitivo con Spark SQL su grossa scala" diventa **molto più sostenibile sui dati cluster** che su quelli locali.

---

## 7. Commento critico

### 7.1 Espressività delle tecnologie

L'espressività cresce nettamente passando da MapReduce a SQL:

- **MapReduce**: il programmatore scrive a mano la logica di partizionamento (`stream.num.map.output.key.fields`), serializzazione tra mapper e reducer (formato `key<TAB>value\n` con JSON nei valori), aggregazione (in-mapper combiner manuale via `dict`). È il modello più "basso livello" disponibile, e ogni operazione semantica (anche banale, come un `GROUP BY` su chiave composta) richiede attenzione esplicita ai dettagli del framework.

- **Spark Core (RDD)**: le operazioni base sono primitive di alto livello (`reduceByKey`, `groupByKey`, `map`), ma il programmatore resta responsabile dell'algoritmo: niente optimizer, niente schema, niente conoscenza dei tipi a runtime. Il codice è 1/3 di MR ma resta procedurale.

- **Spark SQL** e **HiveQL**: la logica è espressa in modo dichiarativo (`GROUP BY`, `COUNT`, `COLLECT_LIST(STRUCT(...))`). Niente codice di shuffle, niente algoritmo da progettare. Il programmatore descrive **cosa** vuole, il framework decide **come** eseguirlo.

Tra Spark SQL e HiveQL la differenza è sottile: stesse parole-chiave, stesse aggregazioni native, stessi tipi composti. Spark SQL su DataFrame ha l'attrattiva del paradigma lazy (puoi comporre `df1 = df.groupBy(...)`, `df2 = df1.filter(...)` senza eseguire nulla, e l'optimizer vede l'intero piano). HiveQL richiede che la stessa logica sia espressa in una **singola query** o che si materializzino tabelle intermedie esplicite, perdendo l'ottimizzazione cross-statement.

### 7.2 Semplicità implementativa

La dimensione del codice nostro racconta il quadro:

**Tabella 6 — Linee di codice "non vuote, non commento" per le 4 implementazioni del job 1.**

| Framework | LOC | File coinvolti |
|---|---|---|
| MapReduce | ~190 | 4 file Python (stage1_mapper, stage1_reducer, stage2_mapper, stage2_reducer) + 1 run.sh |
| Spark Core | ~110 | 1 file Python + 1 run.sh |
| Spark SQL | ~30 | 1 file Python + 1 run.sh |
| HiveQL (pipelined) | ~30 | 1 file HQL + 1 run.sh |

Il rapporto **MR : Spark SQL ≈ 6:1** misura quanto della complessità è data dal framework, non dal problema.

Una nota va fatta sulla **complessità nascosta** di HiveQL: il codice è 30 righe ma il setup non è banale. Su HiveQL abbiamo dovuto affrontare:

1. La trappola dell'**OpenCSVSerde tipi-trasparente** (tutte le colonne lette come stringhe, MIN/MAX lessicografico anziché numerico finché non si introducono i `CAST` espliciti). Questo è un errore semantico silenzioso che produce risultati sbagliati senza alcun warning.
2. La **mancata interpolazione delle variabili** (`${hivevar:...}`) nelle clausole `LOCATION` e `INSERT OVERWRITE DIRECTORY`: si è dovuto usare uno script `sed` lato shell per pre-elaborare l'HQL prima di passarlo a `beeline`.
3. Il **JsonSerDe path** diverso su EMR (`/usr/lib/hive/lib/hive-hcatalog-core.jar`) rispetto all'installazione locale (`/Users/.../hive-4.0.1/hcatalog/...`).
4. La necessità di una **tabella esterna intermedia** per il dump JSON: `INSERT OVERWRITE DIRECTORY` con JsonSerDe non propaga i nomi delle colonne, generando un output con campi `_col0`, `_col1`. Si è dovuto introdurre una tabella esterna `EXTERNAL TABLE ... ROW FORMAT SERDE 'JsonSerDe'` con schema esplicito.

In sostanza: il **codice SQL** è breve, ma la **catena tecnologica** richiede una serie di accomodamenti per produrre l'output desiderato in modo confrontabile con gli altri framework.

Spark SQL non ha avuto problemi simili: la conversione DataFrame → JSON via `.write.json()` è una primitiva diretta del framework, senza dipendenze esterne.

### 7.3 Efficienza

Per dataset di queste dimensioni (<1 GB) e su single-node, **MapReduce custom batte HiveQL** sul tempo assoluto (76s vs 87.5s di HiveQL pipelined sul size 400%). È un risultato controintuitivo che merita commento.

Il MR scritto per il progetto è **ottimizzato a mano** per questo task specifico: in-mapper combiner aggressivo, schema fisso noto, parsing CSV diretto con `csv.reader` di Python. HiveQL è **generico**: deve poter eseguire qualunque query SQL e quindi paga il costo dell'astrazione (OpenCSVSerde generico, CAST espliciti, optimizer Calcite, 3 DAG Tez nel caso CTAS).

Sul cluster questa relazione si capovolge: MR è il framework **più lento in assoluto** su EMR (150s sul size 400%) perché il modello "2 step EMR separati" amplifica gli overhead. **In produzione su volumi grandi, l'astrazione di Hive/Spark vince**: il programmatore non si potrebbe permettere di ottimizzare a mano un MR custom per ogni query.

Spark SQL è il vincitore assoluto in entrambi gli ambienti. La combinazione lazy-evaluation + Catalyst + AQE (Adaptive Query Execution) consente di collassare partizioni vuote a runtime, scegliere algoritmi di join al volo, e gestire skew. Su dataset piccoli AQE riduce a 1 sola partizione di output (vista nei nostri risultati su `bench_50/100` dove l'output di Spark SQL è 1 singolo file part-).

### 7.4 Scalabilità

Lo scaling factor (rapporto tra tempo sulla size 400% e tempo sulla size 50%) è la misura più importante per discutere la scalabilità:

| Framework | Scaling locale | Scaling cluster |
|---|---|---|
| MapReduce | 1.77× | 1.36× |
| HiveQL pipelined | 2.27× | 1.50× |
| Spark Core | 4.46× | 1.67× |
| Spark SQL | 3.37× | 1.26× |

In locale lo scaling di Spark è alto (Spark Core 4.46×, Spark SQL 3.37×) perché Spark single-process è strettamente proporzionale al volume di dati — niente parallelismo distribuito, niente shuffle via rete. MR e Hive sono dominati dallo startup overhead, e crescono meno velocemente perché la "componente costante" è grande.

In cluster lo scaling crolla per **tutti** i framework: i 4 worker assorbono il volume crescente, e il tempo cresce molto meno che linearmente. Spark SQL su EMR cresce solo 1.26× su un volume 8× — è la dimostrazione più chiara del **valore del distributed compute**.

Sui dati che abbiamo (fino a 883 MB), il cluster sembra **sovra-dimensionato** rispetto al carico: i 4 worker hanno capacity inutilizzata, e il bottleneck si sposta sulla lettura/scrittura S3 (latenza per metadata operations). Per misurare scaling significativo sul cluster, andrebbero usate size ben oltre il 400% (dataset multi-GB o multi-TB), ma esula dalle disponibilità computazionali del progetto.

### 7.5 Impatto di shuffle, aggregazione e preparazione dei dati

**Preparazione dei dati**. La scelta di centralizzare la prep in un unico stage Spark (Sezione 3) ha avuto due effetti misurabili: (i) **riduzione del 65% della dimensione su disco** del dataset di lavoro, con beneficio diretto sui tempi di lettura di tutti i job a valle; (ii) **uniformità di schema** tra i quattro framework — tutti partono dallo stesso CSV header-less a 12 colonne e adottano la stessa convenzione di interpretazione dei nulli (record con `op_unique_carrier`/`origin` mancanti già rimossi). Senza questo stadio di prep, ciascun job dovrebbe replicare la logica di pulizia inline, con il rischio di divergenze tra implementazioni.

**Aggregazione**. La prima aggregazione (`GROUP BY (carrier, origin)`) produce ~1500 record da ~7M input — una **riduzione di volume di tre ordini di grandezza** prima del secondo shuffle. Tutti i framework con un combiner / in-mapper aggregation sfruttano questa riduzione efficacemente. MR custom lo fa esplicitamente nel `dict` Python. Spark Core via `reduceByKey` (combinato lato map). Spark SQL via Catalyst (`partial_count`, `partial_min`, ecc. che pre-aggregano lato mapper). HiveQL via `hive.map.aggr=true` (default), che attiva l'aggregazione mappa-lato.

**Shuffle**. Il volume di shuffle tra il primo e il secondo `GROUP BY` è dominato dai ~1500 record, e per Spark/Hive sta nel range dei MB anche sul size 400%. Su questa workload **lo shuffle non è il bottleneck**: il bottleneck è la lettura iniziale del CSV (sequential disk read sul locale, S3 GET su cluster). Su workload con join multi-way o aggregazioni più costose, il discorso sarebbe completamente diverso.

**Materializzazione intermedia in HiveQL**. La differenza misurata tra le due versioni di HiveQL (3 stage CTAS vs 1 stage pipelined) è l'esempio più chiaro dell'**impatto della materializzazione**: la versione CTAS scrive su disco la tabella intermedia `stats_per_carrier_origin` (~10% del volume di input) in formato ORC, poi la rilegge per il secondo stage. La versione pipelined evita questa scrittura intermedia tenendo i risultati in memoria nei vertex Tez. Il guadagno è del **+34% sul size 400%** — proporzionalmente al volume materializzato.

Il punto generale: **Calcite di Hive ottimizza all'interno di una singola query, non tra statement separate**. La materializzazione esplicita di tabelle intermedie con CTAS è una barriera di ottimizzazione: il programmatore deve essere consapevole della scelta. È un'asimmetria importante rispetto a Spark SQL, dove la lazy evaluation di DataFrame elimina del tutto questo problema (Catalyst vede sempre l'intero DAG fino al primo *action*).

---

## 8. Riproducibilità

Tutto il codice e gli artefatti del bench sono nel repository pubblico **[TODO: link GitHub]**. La struttura del repo è:

```
secondo-progetto/
├── prep/                      stage di prep + replica
├── first_job/                 4 implementazioni del job 1
│   ├── MapReduce/             mapper/reducer Python + run.sh
│   ├── hiveQL/                first-job(.hql, -pipelined.hql) + run.sh
│   ├── sparkCore/             first-job.py + run.sh
│   └── sparkSQL/              first-job.py + run.sh
├── second_job/                [TODO completare con il collega]
├── bench/                     orchestratore, cleanup, plot
├── metrics/
│   ├── first_job/             results.csv + plots + summary.md
│   └── first_job_emr/         results.csv (run cluster)
└── report/
    ├── first_job/snippets/    prime 10 righe per ogni framework × size
    └── REPORT.md              questo documento
```

Le istruzioni per riprodurre il bench in locale (assumendo Hadoop 3.4 + Hive 4.0 + Spark 3.5 installati) sono in `README.md` **[TODO scriverlo]**. Le note operative sui fix non-ovvi (in particolare: redirect Derby home, `--add-opens` Java 11 per Tez, conflitto Guava tra Hive e Hadoop, percorso JsonSerDe diverso su EMR) sono raccolte nel README sotto la sezione "Setup pitfalls".

---

## 9. Conclusioni

**[TODO: completare quando il secondo job sarà pronto. Scheletro di partenza:]**

Le quattro tecnologie sperimentate coprono uno spettro ampio in termini di **livello di astrazione**, **espressività**, **efficienza** e **scalabilità**:

- **MapReduce** offre controllo fine ma costo di sviluppo elevato. Su questo bench batte HiveQL in locale per via dell'ottimizzazione manuale, ma è penalizzato fortemente sul cluster dall'overhead di step EMR multi-stage.
- **HiveQL su Tez** ha la massima espressività SQL, ma richiede attenzione alla scrittura della query (CTAS vs pipelined fa il 34% di differenza). È l'unico framework che migliora chiaramente passando da locale a cluster sui dataset più grandi.
- **Spark Core (RDD)** è un compromesso: API più di alto livello di MR, ma senza optimizer. Adatto a logiche dove il programmatore vuole controllo esplicito ma in API moderna.
- **Spark SQL** vince in pratica su entrambi gli ambienti: tempi più bassi, scaling factor migliore, codice più conciso. È la scelta default ragionevole per workload SQL-like.

La differenza più importante che il bench ha messo in luce non è "quale framework è più veloce", ma **come la scalabilità cambia spostando le esecuzioni da single-node a cluster distribuito**. In locale gli scaling factor sono alti (3–4×), in cluster scendono a 1.3–1.7×: il valore del compute distribuito è esattamente questo, e si manifesta in modo molto più chiaro sui dati cluster che non sui dati locali.

---

**[TODO Appendice / acknowledgments / firme]**
