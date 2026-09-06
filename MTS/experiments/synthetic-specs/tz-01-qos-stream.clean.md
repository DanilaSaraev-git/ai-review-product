# ТЗ. Поток событий качества обслуживания (QoS) сети радиодоступа

## Общие сведения

Поток обеспечивает сбор событий качества обслуживания абонентских сессий сетей 4G/5G. Источник событий — платформа PROVIDER_QOS, агрегирующая счётчики сессий с элементов сети радиодоступа. Данные поступают в Data Lake и используются для контроля деградаций и планирования ёмкости сети.

## Решаемая проблема

Сейчас показатели качества сессий доступны только в вендорских системах мониторинга с горизонтом хранения 7 суток и без привязки к абоненту. Из-за этого разбор жалоб на качество связи требует ручных выгрузок. Поток создаётся, чтобы получить единое посуточное хранение событий QoS в Data Lake.

## Продуктовые метрики

- Задержка доставки события в слой RAW: p95 ≤ 30 сек, где начало интервала — время записи события в топик-источник (Kafka log append time), конец — время коммита микропакета в TABLE_QOS_RAW
- Пропускная способность: до 60 000 событий/сек на кластер
- Доля событий с заполненным FIELD_CELL_ID: не менее 95% за календарные сутки

## Заказчики

Network Quality Unit X

## Нефункциональные требования

- Потоковая обработка: событие доступно в TABLE_QOS_RAW не позднее p95 ≤ 30 сек и p99 ≤ 120 сек от момента записи в топик-источник
- Запись в RAW микропакетами длительностью 15 минут внутри часовой партиции FIELD_HOUR; отдельного 15-минутного уровня партиционирования не создаётся
- Географическое разбиение по регионам: один топик-источник на регион, регион переносится в партицию FIELD_REGION
- Хранение: Kafka — 24 ч, RAW-слой — 90 дней от бизнес-даты события

## Системы-источники

PROVIDER_QOS — мультивендорная платформа сбора счётчиков качества сессий 4G/5G.

## Data Catalog

- TABLE_QOS_RAW — LINK_DATACATALOG_QOS_RAW
- TABLE_QOS_DDS_SESSION — LINK_DATACATALOG_QOS_DDS

## Исходники проекта

LINK_GITLAB_QOS_STREAM

## Команда

- USER_A — Product Owner
- USER_B — аналитик
- USER_C — разработчик

## JIRA

PROJECT-QOS-4417 — Поток событий QoS, LINK_JIRA_QOS

## Источники данных

Кластер Kafka источников: CLUSTER_KAFKA_NET_PROD. Сериализация всех топиков-источников: Avro, схема в Schema Registry SCHEMA_REGISTRY_NET, subject SUBJECT_QOS_EVENT_V1, режим совместимости BACKWARD.

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| --- | --- | --- | --- |
| События QoS, регион Центр | Kafka, CLUSTER_KAFKA_NET_PROD | TOPIC_QOS_CENTRAL | Avro, SUBJECT_QOS_EVENT_V1 |
| События QoS, регион Северо-Запад | Kafka, CLUSTER_KAFKA_NET_PROD | TOPIC_QOS_NW | Avro, SUBJECT_QOS_EVENT_V1 |
| События QoS, регион Поволжье | Kafka, CLUSTER_KAFKA_NET_PROD | TOPIC_QOS_VOLGA | Avro, SUBJECT_QOS_EVENT_V1 |
| События QoS, регион Сибирь | Kafka, CLUSTER_KAFKA_NET_PROD | TOPIC_QOS_SIB | Avro, SUBJECT_QOS_EVENT_V1 |
| События QoS, регион Юг | Kafka, CLUSTER_KAFKA_NET_PROD | TOPIC_QOS_SOUTH | Avro, SUBJECT_QOS_EVENT_V1 |
| События QoS, регион Урал | Kafka, CLUSTER_KAFKA_NET_PROD | TOPIC_QOS_URAL | Avro, SUBJECT_QOS_EVENT_V1 |

## Источники обогащения данных

Не применимо. На потоке справочное обогащение не выполняется: соты и вендоры присоединяются на слое DDS в рамках отдельного ТЗ.

| Описание источника | Ссылка | Описание |
| --- | --- | --- |
| Не применимо | — | Обогащение на потоке не выполняется |

## Приемники данных

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| --- | --- | --- | --- |
| TABLE_QOS_RAW — необработанные события QoS | CLUSTER_HADOOP_NET_PROD | LINK_DATACATALOG_QOS_RAW | ORC, компрессия zlib |
| TABLE_QOS_DDS_SESSION — детальные сессии | CLUSTER_HADOOP_NET_PROD | LINK_DATACATALOG_QOS_DDS | ORC, компрессия zlib |

Полный путь хранения RAW: `hdfs://CLUSTER_HADOOP_NET_PROD/data/net/qos/raw/qos_event/`, формат ORC, компрессия zlib, структура каталогов `field_region=<region>/field_event_date=<YYYY-MM-DD>/field_hour=<0..23>/`.

## Схема потоков данных

PROVIDER_QOS → Kafka (топики по регионам, CLUSTER_KAFKA_NET_PROD) → Apache Flink → RAW → DDS

Мониторинг: LINK_DASHBOARD_QOS

<!-- конец страницы 1 -->

## Алгоритм обработки потока

### Шаг 1. Приведение времени

Платформа PROVIDER_QOS передаёт метки начала и окончания сессии в местном времени региона и отдельно передаёт смещение этого времени относительно UTC в минутах (атрибут источника `tz_offset_min`, положительное значение — восточнее UTC). При обработке из местного времени вычитается смещение, обе метки сохраняются в UTC. Значение смещения сохраняется в FIELD_TZ_OFFSET_MIN без изменений. Если смещение не передано, применяется смещение региона из конфигурации потока, а в FIELD_TZ_OFFSET_MIN записывается NULL.

FIELD_EVENT_DATE и FIELD_HOUR вычисляются из FIELD_SESSION_START после приведения к UTC.

### Шаг 2. Фильтрация данных

Фильтрация выполняется после шага 1, то есть по значениям в UTC. Событие принимается, если FIELD_SESSION_START попадает в интервал от «время обработки минус 7 суток» до «время обработки плюс 2 часа», где время обработки — системное время UTC на узле обработки. События вне интервала направляются в поток отбраковки TOPIC_QOS_REJECTED и в TABLE_QOS_RAW не попадают.

Событие также отбраковывается, если FIELD_SESSION_START пуст или не разбирается как метка времени.

Тестовые сессии исключаются по условию: FIELD_IMSI входит в перечень тестовых IMSI из справочника TABLE_TEST_IMSI_REF, версия справочника — актуальная на момент обработки события.

### Шаг 3. Нормализация идентификаторов

Служебные префиксы оператора удаляются до первого значимого символа в полях FIELD_IMSI, FIELD_MSISDN и FIELD_SESSION_ID. Правила краевых значений: пустая строка сохраняется как пустая строка, NULL сохраняется как NULL, строка из одних нулей приводится к значению «0».

### Шаг 4. Расчёт производных показателей

FIELD_THROUGHPUT_DL_KBPS = FIELD_DL_VOLUME_BYTES × 8 / 1024 / длительность сессии в секундах, где длительность — разность FIELD_SESSION_END и FIELD_SESSION_START в UTC. Если FIELD_SESSION_END не заполнен или длительность равна нулю, FIELD_THROUGHPUT_DL_KBPS записывается как NULL.

## Формирование ключа (kafka) / партиции (hdfs)

- Ключ сообщения в топиках-источниках: FIELD_SESSION_ID; порядок событий гарантируется в пределах одной сессии.
- Партиционирование в HDFS: FIELD_REGION, FIELD_EVENT_DATE, FIELD_HOUR; значения дат и часов — в UTC.

## Структура данных

Таблица: TABLE_QOS_RAW

| Комментарий | Атрибут | Тип данных | Обязательность | Описание атрибута | Источник | Атрибут источника |
| --- | --- | --- | --- | --- | --- | --- |
| Партиция | FIELD_REGION | string | NOT NULL | Регион | PROVIDER_QOS | region |
| Партиция | FIELD_EVENT_DATE | date | NOT NULL | Дата события (UTC) | Расчётное, шаг 1 | — |
| Партиция | FIELD_HOUR | int | NOT NULL | Час события (UTC) | Расчётное, шаг 1 | — |
| | FIELD_IMSI | string | NOT NULL | IMSI абонента | PROVIDER_QOS | imsi |
| | FIELD_MSISDN | string | NULLABLE | MSISDN абонента | PROVIDER_QOS | msisdn |
| Ключ сессии | FIELD_SESSION_ID | string | NOT NULL | Идентификатор сессии | PROVIDER_QOS | session_id |
| | FIELD_CELL_ID | long | NULLABLE | Идентификатор соты | PROVIDER_QOS | cell_id |
| | FIELD_ENODEB_ID | long | NULLABLE | Идентификатор базовой станции | PROVIDER_QOS | enb_id |
| | FIELD_RAT_TYPE | string | NULLABLE | Технология доступа: LTE, NR | PROVIDER_QOS | rat_type |
| | FIELD_QCI | int | NULLABLE | Класс качества обслуживания | PROVIDER_QOS | qci |
| | FIELD_DL_VOLUME_BYTES | bigint | NULLABLE | Объём трафика вниз, байт | PROVIDER_QOS | dl_bytes |
| | FIELD_UL_VOLUME_BYTES | bigint | NULLABLE | Объём трафика вверх, байт | PROVIDER_QOS | ul_bytes |
| Расчётное, шаг 4 | FIELD_THROUGHPUT_DL_KBPS | double | NULLABLE | Скорость вниз, Кбит/с | Расчётное | — |
| | FIELD_LATENCY_MS | int | NULLABLE | Средняя задержка сессии, мс | PROVIDER_QOS | latency_ms |
| | FIELD_PACKET_LOSS_PCT | double | NULLABLE | Доля потерянных пакетов, % | PROVIDER_QOS | loss_pct |
| UTC, шаг 1 | FIELD_SESSION_START | timestamp | NOT NULL | Время начала сессии | PROVIDER_QOS | sess_start |
| UTC, шаг 1 | FIELD_SESSION_END | timestamp | NULLABLE | Время окончания сессии | PROVIDER_QOS | sess_end |
| | FIELD_TZ_OFFSET_MIN | int | NULLABLE | Смещение местного времени региона относительно UTC, мин | PROVIDER_QOS | tz_offset_min |
| | FIELD_TERMINATION_CAUSE | int | NULLABLE | Код завершения сессии | PROVIDER_QOS | term_cause |
| | FIELD_VENDOR | string | NULLABLE | Вендор оборудования БС | PROVIDER_QOS | vendor |

## Пример данных

Макрос «Раскрыть», 10 строк из TABLE_QOS_RAW.

| FIELD_REGION | FIELD_EVENT_DATE | FIELD_HOUR | FIELD_SESSION_ID | FIELD_RAT_TYPE | FIELD_QCI | FIELD_DL_VOLUME_BYTES | FIELD_LATENCY_MS |
| --- | --- | --- | --- | --- | --- | --- | --- |
| central | 2026-08-14 | 9 | SESS_0000000001 | LTE | 9 | 154321 | 42 |
| central | 2026-08-14 | 9 | SESS_0000000002 | LTE | 8 | 8842190 | 55 |
| central | 2026-08-14 | 10 | SESS_0000000003 | NR | 9 | 0 | 130 |
| nw | 2026-08-14 | 10 | SESS_0000000004 | LTE | 6 | 512000 | 38 |
| nw | 2026-08-14 | 11 | SESS_0000000005 | LTE | 9 | 1048576 | 47 |
| volga | 2026-08-14 | 11 | SESS_0000000006 | NR | 9 | 220144 | 61 |
| sib | 2026-08-14 | 12 | SESS_0000000007 | LTE | 5 | 76500 | 29 |
| sib | 2026-08-14 | 12 | SESS_0000000008 | LTE | 9 | 3391002 | 88 |
| south | 2026-08-14 | 13 | SESS_0000000009 | NR | 9 | 44100 | 150 |
| ural | 2026-08-14 | 13 | SESS_0000000010 | LTE | 8 | 12058320 | 34 |

## DDL

```sql
CREATE TABLE SCHEMA_RAW.TABLE_QOS_RAW (
  FIELD_IMSI                STRING    NOT NULL,
  FIELD_MSISDN              STRING,
  FIELD_SESSION_ID          STRING    NOT NULL,
  FIELD_CELL_ID             BIGINT,
  FIELD_ENODEB_ID           BIGINT,
  FIELD_RAT_TYPE            STRING,
  FIELD_QCI                 INT,
  FIELD_DL_VOLUME_BYTES     BIGINT,
  FIELD_UL_VOLUME_BYTES     BIGINT,
  FIELD_THROUGHPUT_DL_KBPS  DOUBLE,
  FIELD_LATENCY_MS          INT,
  FIELD_PACKET_LOSS_PCT     DOUBLE,
  FIELD_SESSION_START       TIMESTAMP NOT NULL,
  FIELD_SESSION_END         TIMESTAMP,
  FIELD_TZ_OFFSET_MIN       INT,
  FIELD_TERMINATION_CAUSE   INT,
  FIELD_VENDOR              STRING
)
PARTITIONED BY (FIELD_REGION STRING, FIELD_EVENT_DATE DATE, FIELD_HOUR INT)
STORED AS ORC
LOCATION 'hdfs://CLUSTER_HADOOP_NET_PROD/data/net/qos/raw/qos_event/'
TBLPROPERTIES ('orc.compress'='ZLIB');
```

## FAQ

**Как определить технологию связи сессии (4G/5G)?**
По полю FIELD_RAT_TYPE: LTE соответствует 4G, NR — 5G. Если поле не заполнено, технология в рамках этого потока не определяется и сохраняется значение NULL.

**Почему объём трафика может быть равен нулю?**
Событие создаётся и для сессий, завершившихся без передачи данных; такие строки сохраняются в RAW без изменений, FIELD_THROUGHPUT_DL_KBPS для них равен нулю.

**В каком времени хранятся метки?**
Все метки времени в TABLE_QOS_RAW хранятся в UTC после приведения на шаге 1; исходное смещение доступно в FIELD_TZ_OFFSET_MIN.

## История изменений

| Дата | Версия | Изменение | Автор |
| --- | --- | --- | --- |
| 2026-08-03 | 0.1 | Первая редакция | USER_B |
| 2026-08-14 | 0.2 | Добавлены поля задержки и потерь пакетов | USER_B |
| 2026-08-21 | 0.3 | Уточнены сериализация, кластеры, путь HDFS, правила времени и фильтрации | USER_B |
