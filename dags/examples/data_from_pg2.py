import logging
import psycopg2
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)


def say_hello(log: logging.Logger) -> None:
    log.info("Hello Worlds!!")


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'example', 'hello_world'],
    is_paused_upon_creation=False
)
def hello_world_dag():

    @task()
    def hello_task():
        say_hello(log)

    @task()
    def load_ranks():
        psql_conn = BaseHook.get_connection('PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
        conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
        cur = conn.cursor()
        df = cur.execute("select * from ranks")
        cur.close()
        conn.close()

        postgres_hook = PostgresHook('PG_WAREHOUSE_CONNECTION')
        engine = postgres_hook.get_sqlalchemy_engine()
        row_count = df.to_sql('bonussystem_ranks', engine, schema='stg', if_exists='replace', index=False)
        print(f'{row_count} вставлено.')

    hello = hello_task()
    rank = load_ranks()
    hello >> rank


hello_dag = hello_world_dag()
