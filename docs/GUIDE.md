# Пошаговая инструкция: что и где посмотреть в проекте

Инструкция для того, чтобы своими руками пройти по всем инструментам проекта и понять, что каждый из них делает. По времени это 1–1,5 часа. Каждый шаг заканчивается вопросом, который могут задать на собеседовании, и ответом на него.

Общая картина:

```
ЦБ РФ (API) ──► Airflow загружает ──► PostgreSQL: raw → stg → dds ──► dbt строит mart ──► ClickHouse
                    (Python)              (проверки DQ, MERGE, SCD2)      (модели + тесты)     (аналитика)
Всё это работает в контейнерах Docker, а код хранится в Git на GitHub.
```

---

## 0. Запуск

1. Запустите **Docker Desktop** (значок кита в трее должен стать зелёным).
2. Откройте терминал в папке проекта и выполните:
   ```powershell
   cd D:\Projects\dwh-airflow-pet
   docker compose up -d
   ```
3. Подождите около минуты.

---

## 1. Docker: из чего состоит проект

**Где смотреть:** Docker Desktop → вкладка **Containers** → группа `dwh-airflow-pet`.

| Контейнер | Что это | Статус |
|---|---|---|
| `postgres` | PostgreSQL 16: хранилище (база `dwh`) и служебная база Airflow | Running |
| `clickhouse` | Колоночная СУБД для аналитики | Running |
| `airflow-webserver` | Веб-интерфейс Airflow | Running |
| `airflow-scheduler` | Планировщик Airflow: запускает задачи по расписанию | Running |
| `airflow-init` | Одноразовый: создаёт базу Airflow и пользователя admin | Exited — это нормально |

**Что потрогать:**
- Нажмите на `airflow-scheduler` → вкладка **Logs**: здесь видно, как планировщик запускает задачи.
- Откройте вкладку **Exec** (терминал внутри контейнера) и выполните `ls /opt/airflow`: там лежат папки `dags`, `cbr_etl`, `sql`, `dbt`. Это ваши файлы с диска D:, подключённые в контейнер (volumes). Если поправить DAG на диске, Airflow увидит изменения без пересборки образа.
- Откройте в редакторе `docker-compose.yml` и найдите `services`, `ports`, `volumes`, `depends_on` и `healthcheck`.
- Откройте `Dockerfile`: в нём образ Airflow плюс ClickHouse-клиент и dbt в отдельном virtualenv.

Полезные команды:
```powershell
docker compose ps                     # статус контейнеров
docker compose logs -f airflow-scheduler   # логи в реальном времени
docker compose down                   # остановить (данные сохранятся в volumes)
docker compose down -v                # остановить и удалить данные: всё создастся заново
```

> **Вопрос:** зачем Docker в DWH-проекте?
> **Ответ:** окружение одинаково у всех. Одна команда поднимает PostgreSQL, ClickHouse и Airflow нужных версий, и проблема «у меня работает, а у тебя нет» исчезает.

---

## 2. Airflow: оркестрация

**Где смотреть:** http://localhost:8080, логин `admin`, пароль `admin`.

### 2.1. Список DAG'ов
На главной странице четыре DAG'а:

| DAG | Расписание (Schedule) | Что делает |
|---|---|---|
| `cbr_currency_ref` | `@weekly` | Справочник валют → SCD2 |
| `cbr_key_rate` | `@daily` | Ключевая ставка |
| `cbr_currency_rates` | `@daily` | Курсы валют |
| `cbr_dbt` | Dataset | dbt + выгрузка в ClickHouse |

Переключатель слева от DAG'а включает его или ставит на паузу.

### 2.2. Граф задач
Откройте `cbr_currency_rates` → вкладка **Graph**. Там видна цепочка: `extract → stage → quality → load_dds`.
- Нажмите на квадратик задачи → **Logs**. В логе `quality` видны строки вида `DQ currency_rate.positive_values passed`.
- Нажмите на задачу → **XCom**: здесь видно, как `extract` передал `load_id` следующей задаче.

### 2.3. Grid: история запусков
Вкладка **Grid**: каждый столбец — один день, начиная с 1 января 2026 года. Это **catchup/бэкфилл**: Airflow сам догрузил историю за все прошлые даты. Зелёный цвет означает успех, красный — ошибку.

### 2.4. Перезапуск (идемпотентность)
1. В Grid выберите любой старый день → задачу `extract` → **Clear** → **Clear downstream**.
2. Airflow перезапустит цепочку за этот день.
3. Убедитесь в DBeaver (шаг 3), что количество строк в `dds.fct_currency_rate` не изменилось. Это и есть идемпотентность: повторный запуск не создаёт дублей.

### 2.5. Datasets
Меню **Datasets**: два датасета, `fct_currency_rate` и `fct_key_rate`. DAG `cbr_dbt` запускается, когда оба факта обновились. Это связь между DAG'ами по данным, а не по времени.

### 2.6. Ручной запуск
`cbr_currency_ref` → кнопка ▶ **Trigger DAG**.

> **Вопрос:** чем `catchup` отличается от ретраев?
> **Ответ:** catchup запускает DAG за прошлые интервалы, которые были пропущены. Retries перезапускают упавшую задачу внутри одного интервала. В проекте 3 ретрая с экспоненциальной задержкой, а при окончательном падении срабатывает алерт (`on_failure_callback`).

---

## 3. PostgreSQL: слои хранилища

**Чем смотреть:** [DBeaver Community](https://dbeaver.io/download/) (бесплатный).
Подключение: PostgreSQL, host `localhost`, port `5432`, database `dwh`, user `dwh`, password `dwh`.

Пройдите по схемам **сверху вниз**, так же, как движутся данные:

**`raw.cbr_response`** — ответ API как есть:
```sql
SELECT load_id, source, request_params, left(payload, 300), loaded_at
FROM raw.cbr_response ORDER BY load_id DESC LIMIT 10;
```
Зачем: если парсер ошибся, данные можно перегрузить, не обращаясь к ЦБ повторно.

**`stg.currency_rate`** — разобранные и типизированные строки, у каждой есть `load_id`:
```sql
SELECT * FROM stg.currency_rate WHERE char_code = 'USD' ORDER BY load_id DESC LIMIT 5;
```

**`meta.dq_result`** — журнал проверок качества:
```sql
SELECT dataset, check_name, severity, failed_rows, passed, checked_at
FROM meta.dq_result ORDER BY id DESC LIMIT 20;
```

**`dds.fct_currency_rate`** — факт, ключ `(rate_date, currency_id)`, загружается через `MERGE`:
```sql
SELECT rate_date, char_code, nominal, value, vunit_rate, row_hash, updated_at
FROM dds.fct_currency_rate WHERE char_code IN ('USD','EUR','CNY')
ORDER BY rate_date DESC LIMIT 9;
```
`row_hash` — md5 от атрибутов строки. MERGE обновляет строку, только если хэш изменился (это CDC).

**`dds.dim_currency`** — SCD Type 2:
```sql
SELECT currency_id, char_code, name, valid_from, valid_to, is_current
FROM dds.dim_currency WHERE char_code = 'USD';
```

**Опыт со SCD2 своими руками:**
1. Испортите название, чтобы «источник» отличался от хранилища:
   ```sql
   UPDATE dds.dim_currency SET row_hash = 'x' WHERE char_code = 'USD' AND is_current;
   ```
2. В Airflow запустите `cbr_currency_ref` (▶ Trigger).
3. Снова выполните запрос к `dim_currency` по USD: теперь у валюты **две версии**. Старая закрыта (`valid_to` равен дате загрузки, `is_current = false`), новая открыта до `9999-12-31`.

SQL всех шагов лежит в папке `sql/dml/`: `rates_stg_to_dds.sql`, `currency_ref_scd2.sql`, `key_rate_stg_to_dds.sql`.

> **Вопрос:** зачем слои raw/stg/dds/mart?
> **Ответ:** у каждого слоя своя ответственность. raw — точная копия источника для перезагрузки, stg — типизация и проверки, dds — очищенная история и ключи, mart — витрины под задачи бизнеса. Ошибку можно локализовать по слою.

---

## 4. dbt: витрины и тесты

Код лежит в папке `dbt/`:
```
dbt/
├── dbt_project.yml          # настройки: staging → view, marts → table
├── profiles.yml             # подключение к PostgreSQL
├── macros/                  # свой generic-тест unique_combination
├── models/
│   ├── staging/             # обёртки над dds (_sources.yml — источники и их тесты)
│   └── marts/               # витрины + _marts.yml с описаниями и тестами
└── tests/                   # singular-тесты (SQL, который должен вернуть 0 строк)
```

**Запуск руками:** откройте в Docker Desktop контейнер `airflow-webserver` → **Exec** и выполните:
```bash
cd /opt/airflow/dbt
dbt build                                   # все модели и тесты в правильном порядке
dbt build --select currency_rate_daily      # одна модель и её тесты
dbt build --select +currency_rate_monthly   # модель и всё, от чего она зависит
dbt test --select source:*                  # только тесты источников
dbt source freshness                        # свежесть данных
```
В выводе видно `OK created ... model mart.currency_rate_daily` и `PASS` по каждому тесту.

**Документация и граф зависимостей (lineage)** — это самое наглядное в dbt:
```bash
cd /opt/airflow/dbt
dbt docs generate
dbt docs serve --port 8081 --host 0.0.0.0
```
Откройте http://localhost:8081 и нажмите синюю круглую кнопку в правом нижнем углу. Появится граф: `source → stg_* → currency_rate_daily → currency_rate_monthly`. В терминале остановите сервер клавишами Ctrl+C.

**На что посмотреть в коде:**
- `models/marts/currency_rate_daily.sql`: **инкрементальная** модель (`materialized='incremental'`, `delete+insert`). Блок `{% if is_incremental() %}` пересчитывает только последние 7 дней. Есть оконные функции (lag, скользящие средние) и `LATERAL` для ставки на дату.
- `models/marts/key_rate_periods.sql`: задача gaps and islands (периоды одинаковой ставки).
- `{{ ref('...') }}` и `{{ source('...') }}`: dbt сам вычисляет порядок запуска моделей.

**Опыт: сломать тест.** В DBeaver выполните:
```sql
UPDATE mart.currency_rate_daily SET change_abs = 999 WHERE char_code = 'USD' AND rate_date = (SELECT max(rate_date) FROM mart.currency_rate_daily);
```
Затем выполните `dbt test --select currency_rate_daily`: тест `assert_daily_change_consistent` упадёт (**FAIL 1**). Команда `dbt build` пересоберёт витрину, и тест снова пройдёт.

> **Вопрос:** зачем dbt, если можно писать SQL-скрипты?
> **Ответ:** dbt даёт зависимости между моделями через `ref`, тесты, документацию и lineage, инкрементальные модели и версионирование в Git. SQL остаётся SQL, но становится похож на программный проект.

---

## 5. ClickHouse: аналитика

**Где смотреть:** http://localhost:8123/play, user `dwh`, password `dwh` (поля вверху справа).
Можно подключиться и в DBeaver: тип ClickHouse, port `8123`.

```sql
-- сколько строк выгружено и до какой даты
SELECT count(), min(rate_date), max(rate_date) FROM cbr.currency_rate_daily FINAL;

-- курс USD/EUR/CNY по месяцам
SELECT toStartOfMonth(rate_date) AS month, char_code, round(avg(vunit_rate), 2) AS avg_rate
FROM cbr.currency_rate_daily FINAL
WHERE char_code IN ('USD','EUR','CNY')
GROUP BY month, char_code ORDER BY month, char_code;

-- как устроена таблица
SHOW CREATE TABLE cbr.currency_rate_daily;

-- партиции по месяцам
SELECT partition, sum(rows) FROM system.parts
WHERE table = 'currency_rate_daily' AND active GROUP BY partition ORDER BY partition;
```

**Что понять:**
- `ENGINE = ReplacingMergeTree(updated_at)`: если строка выгружена повторно, ClickHouse оставит версию с самым свежим `updated_at`. `FINAL` в запросе схлопывает дубли сразу.
- `ORDER BY (char_code, rate_date)`: это первичный ключ и сортировка на диске, запросы по валюте и дате работают быстро.
- Выгрузка инкрементальная: `cbr_etl/clickhouse.py` берёт в PostgreSQL только строки с `updated_at` больше максимального в ClickHouse (watermark).

> **Вопрос:** почему аналитику держат в ClickHouse, а не в PostgreSQL?
> **Ответ:** ClickHouse — колоночная MPP-СУБД. Агрегации по миллионам строк в нём в разы быстрее, потому что он читает только нужные колонки и хорошо их сжимает. PostgreSQL удобнее для MERGE, транзакций и точечных обновлений.

---

## 6. Git и GitHub

**Где смотреть:** https://github.com/maxnikoaeww/dwh-airflow-pet

- **Code**: README со схемой архитектуры. Именно его откроет рекрутер.
- **Commits**: история изменений.
- **Actions**: CI. На каждый `git push` GitHub запускает pytest и проверку `docker-compose.yml`. Зелёная галочка означает, что всё прошло.

**Попробуйте сами:**
```powershell
cd D:\Projects\dwh-airflow-pet
git checkout -b feature/usd-chart        # новая ветка
# ... поменяйте что-нибудь, например описание в dbt/models/marts/_marts.yml
git add .
git commit -m "Describe currency_rate_daily columns"
git push -u origin feature/usd-chart
```
Затем на GitHub нажмите **Compare & pull request**. Так в командах вносят изменения через code review.

---

## 7. Сценарий сквозной проверки (5 минут)

1. Airflow → `cbr_currency_rates` → Grid: последний день зелёный.
2. DBeaver: `SELECT max(rate_date) FROM dds.fct_currency_rate;` — вчерашняя или сегодняшняя дата.
3. Airflow → `cbr_dbt`: запуск прошёл после загрузки фактов.
4. DBeaver: `SELECT * FROM mart.currency_rate_daily WHERE char_code='USD' ORDER BY rate_date DESC LIMIT 5;`
5. ClickHouse Play: `SELECT max(rate_date) FROM cbr.currency_rate_daily FINAL;` — та же дата.

Если все пять шагов сходятся, весь пайплайн работает от API до аналитической БД.

---

## Если что-то не работает

| Симптом | Что сделать |
|---|---|
| http://localhost:8080 не открывается | Подождать минуту после запуска, затем `docker compose ps` и `docker compose logs airflow-webserver` |
| DAG красный | Открыть красную задачу → Logs, внизу будет ошибка. Чаще всего это недоступный API ЦБ: задача повторится сама |
| Ошибка `input/output error` или Docker не стартует | Мало места на диске Docker. Освободите место или перенесите хранилище: Docker Desktop → Settings → Resources → Disk image location |
| Хочется начать с нуля | `docker compose down -v`, затем `docker compose up -d` |
