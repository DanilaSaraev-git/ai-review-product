# ТЗ. Поток событий качества обслуживания (QoS) сети радиодоступа

## Общие сведения

Поток обеспечивает сбор событий качества обслуживания абонентских сессий сетей 4G/5G. Источник событий — платформа PROVIDER_QOS, агрегирующая счётчики сессий с элементов сети радиодоступа. Данные поступают в Data Lake и используются для контроля деградаций и планирования ёмкости сети.

## Решаемая проблема

Сейчас показатели качества сессий доступны только в вендорских системах мониторинга с горизонтом хранения 7 суток и без привязки к абоненту. Из-за этого разбор жалоб на качество связи требует ручных выгрузок. Поток создаётся, чтобы получить единое посуточное хранение событий QoS в Data Lake.

## Продуктовые метрики

- Задержка: < 30 сек
- Пропускная способность: до 60 000 событий/сек
- Доля событий с заполненным идентификатором соты: не менее 95%

## Заказчики

Network Quality Unit X

## Нефункциональные требования

- Обработка в реальном времени (задержка ≈ 0 сек)
- Инкрементная загрузка 15-минутными партициями
- Географическое разбиение по регионам
- Хранение: Kafka — 24 ч, RAW-слой — 90 дней

## Системы-источники

PROVIDER_QOS — мультивендорная платформа сбора счётчиков качества сессий 4G/5G.

## Data Catalog

Ссылка будет добавлена после публикации объектов.

## Исходники проекта

LINK_GITLAB_QOS_STREAM

## Команда

- USER_A — Product Owner
- USER_B — аналитик
- USER_C — разработчик

## JIRA

PROJECT-QOS-4417 — Поток событий QoS, LINK_JIRA_QOS

## Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| --- | --- | --- | --- |
| События QoS, регион Центр | Kafka | TOPIC_QOS_CENTRAL | — |
| События QoS, регион Северо-Запад | Kafka | TOPIC_QOS_NW | — |
| События QoS, регион Поволжье | Kafka | TOPIC_QOS_VOLGA | — |
| События QoS, регион Сибирь | Kafka | TOPIC_QOS_SIB | — |
| События QoS, регион Юг | Kafka | TOPIC_QOS_SOUTH | — |
| События QoS, регион Урал | Kafka | TOPIC_QOS_URAL | — |

## Приемники данных

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| --- | --- | --- | --- |
| TABLE_QOS_RAW — необработанные события QoS | прод | — | см. стандарт слоя RAW |
| TABLE_QOS_DDS_SESSION — детальные сессии | прод | — | см. стандарт слоя DDS |

Хранение файлов: RAW-слой, hdfs, каталог qos.

## Схема потоков данных

PROVIDER_QOS → Kafka (топики по регионам) → Apache Flink → RAW → DDS

Мониторинг: LINK_DASHBOARD_QOS

<!-- конец страницы 1 -->

## Алгоритм обработки потока

### Шаг 1. Фильтрация данных

Отбрасываются события с некорректными временными метками. Не учитываются служебные сессии тестовых SIM-карт.

### Шаг 2. Нормализация идентификаторов

Из этих полей удаляются служебные префиксы оператора до первого значимого символа.

### Шаг 3. Расчёт производных показателей

FIELD_THROUGHPUT_DL_KBPS рассчитывается как FIELD_DL_VOLUME_BYTES × 8 / 1024 / длительность сессии в секундах. Длительность определяется по разнице FIELD_SESSION_END и FIELD_SESSION_START.

Временные метки приходят в местном времени региона. Партиционирование выполняется по FIELD_EVENT_DATE.

## Структура данных

Таблица: TABLE_QOS_RAW

| Комментарий | Атрибут | Тип данных | Обязательность | Описание атрибута | Источник | Атрибут источника |
| --- | --- | --- | --- | --- | --- | --- |
| Партиция | FIELD_REGION | string | NOT NULL | Регион | PROVIDER_QOS | region |
| Партиция | FIELD_EVENT_DATE | date | NOT NULL | Дата события | PROVIDER_QOS | event_dt |
| Партиция | FIELD_HOUR | int | NOT NULL | Час события | PROVIDER_QOS | event_hour |
| | FIELD_IMSI | string | NOT NULL | IMSI абонента | PROVIDER_QOS | imsi |
| | FIELD_MSISDN | string | NULLABLE | MSISDN абонента | PROVIDER_QOS | msisdn |
| Ключ сессии | FIELD_SESSION_ID | string | NOT NULL | Идентификатор сессии | PROVIDER_QOS | session_id |
| | FIELD_CELL_ID | long | NULLABLE | Идентификатор соты | PROVIDER_QOS | cell_id |
| | FIELD_ENODEB_ID | long | NULLABLE | Идентификатор базовой станции | PROVIDER_QOS | enb_id |
| | FIELD_QCI | int | NULLABLE | Класс качества обслуживания | PROVIDER_QOS | qci |
| | FIELD_DL_VOLUME_BYTES | bigint | NULLABLE | Объём трафика вниз, байт | PROVIDER_QOS | dl_bytes |
| | FIELD_UL_VOLUME_BYTES | bigint | NULLABLE | Объём трафика вверх, байт | PROVIDER_QOS | ul_bytes |
| Расчётное | FIELD_THROUGHPUT_DL_KBPS | double | NULLABLE | Скорость вниз, Кбит/с | — | — |
| | FIELD_LATENCY_MS | int | NULLABLE | Средняя задержка сессии, мс | PROVIDER_QOS | latency_ms |
| | FIELD_PACKET_LOSS_PCT | double | NULLABLE | Доля потерянных пакетов, % | PROVIDER_QOS | loss_pct |
| | FIELD_SESSION_START | timestamp | NOT NULL | Время начала сессии | PROVIDER_QOS | sess_start |
| | FIELD_SESSION_END | timestamp | NULLABLE | Время окончания сессии | PROVIDER_QOS | sess_end |
| | FIELD_TERMINATION_CAUSE | int | NULLABLE | Код завершения сессии | PROVIDER_QOS | term_cause |
| | FIELD_VENDOR | string | NULLABLE | Вендор оборудования БС | PROVIDER_QOS | vendor |

## Пример данных

Макрос «Раскрыть», 10 строк из TABLE_QOS_RAW.

| FIELD_REGION | FIELD_EVENT_DATE | FIELD_HOUR | FIELD_SESSION_ID | FIELD_QCI | FIELD_DL_VOLUME_BYTES | FIELD_LATENCY_MS |
| --- | --- | --- | --- | --- | --- | --- |
| central | 2026-08-14 | 9 | SESS_0000000001 | 9 | 154321 | 42 |
| central | 2026-08-14 | 9 | SESS_0000000002 | 8 | 8842190 | 55 |
| central | 2026-08-14 | 10 | SESS_0000000003 | 9 | 0 | 130 |
| nw | 2026-08-14 | 10 | SESS_0000000004 | 6 | 512000 | 38 |
| nw | 2026-08-14 | 11 | SESS_0000000005 | 9 | 1048576 | 47 |
| volga | 2026-08-14 | 11 | SESS_0000000006 | 9 | 220144 | 61 |
| sib | 2026-08-14 | 12 | SESS_0000000007 | 5 | 76500 | 29 |
| sib | 2026-08-14 | 12 | SESS_0000000008 | 9 | 3391002 | 88 |
| south | 2026-08-14 | 13 | SESS_0000000009 | 9 | 44100 | 150 |
| ural | 2026-08-14 | 13 | SESS_0000000010 | 8 | 12058320 | 34 |

## DDL

```sql
CREATE TABLE SCHEMA_RAW.TABLE_QOS_RAW (
  FIELD_IMSI                STRING    NOT NULL,
  FIELD_MSISDN              STRING,
  FIELD_SESSION_ID          STRING    NOT NULL,
  FIELD_CELL_ID             BIGINT,
  FIELD_ENODEB_ID           BIGINT,
  FIELD_QCI                 INT,
  FIELD_DL_VOLUME_BYTES     BIGINT,
  FIELD_UL_VOLUME_BYTES     BIGINT,
  FIELD_THROUGHPUT_DL_KBPS  DOUBLE,
  FIELD_LATENCY_MS          INT,
  FIELD_PACKET_LOSS_PCT     DOUBLE,
  FIELD_SESSION_START       TIMESTAMP NOT NULL,
  FIELD_SESSION_END         TIMESTAMP,
  FIELD_TERMINATION_CAUSE   INT,
  FIELD_VENDOR              STRING
)
PARTITIONED BY (FIELD_REGION STRING, FIELD_EVENT_DATE DATE, FIELD_HOUR INT);
```

## FAQ

**Как определить технологию связи сессии (4G/5G)?**
Для сессий передачи данных — по полю radioAccessTechnology. Если поле не заполнено, технология определяется по FIELD_TECH_GEN.

**Почему объём трафика может быть равен нулю?**
Событие создаётся и для сессий, завершившихся без передачи данных; такие строки сохраняются в RAW без изменений.

## История изменений

| Дата | Версия | Изменение | Автор |
| --- | --- | --- | --- |
| 2026-08-03 | 0.1 | Первая редакция | USER_B |
| 2026-08-14 | 0.2 | Добавлены поля задержки и потерь пакетов | USER_B |
