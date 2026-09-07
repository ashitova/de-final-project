## Команды для работы с docker
Старт и остановка контейнеров (из директорий docker-compose.yaml)
```bash
docker compose up -d
docker compose down # без удалением volumes 
docker compose down -v # с удалением volumes 
```
### Для контейнера Airflow
При первом запуске Airflow выполнить:
```bash
cd path-to-airflow/airflow
echo -e "AIRFLOW_UID=$(id -u)" > .env
echo -e "AIRFLOW_PROJ_DIR=$PWD" >> .env
docker compose run airflow-cli airflow config list
docker compose up airflow-init
docker compose up --build -d
```
Создание подключений из Airflow к контейнерам ClickHouse, PostgreSQL, Minio:
```bash
docker exec -it airflow-airflow-scheduler-1 airflow connections add clickhouse_conn \
--conn-type clickhouse \
--conn-host host.docker.internal \
--conn-port 8123 \
--conn-schema analytics \
--conn-login username \
--conn-password password

docker exec -it airflow-airflow-scheduler-1 airflow connections add postgres_conn \
--conn-type postgres \
--conn-host host.docker.internal \
--conn-port 5432 \
--conn-schema postgres \
--conn-login postgres \
--conn-password postgres

docker exec -it airflow-airflow-scheduler-1 airflow connections add minio_conn \
--conn-type aws \
--conn-login myaccesskey \
--conn-password mysecretkey \
--conn-extra '{"endpoint_url": "http://host.docker.internal:9000"}'
```
Синхронизация дагов в Airflow:
```bash
docker exec -it airflow-airflow-scheduler-1 airflow dags reserialize

```