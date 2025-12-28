from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.email import EmailOperator

from dags.datacleaner import data_cleaner


def fetch_dicts(cur):
    """
    Return query results as a list[dict] with column names as keys.
    Also stringifies values (date/Decimal) so XCom + email render cleanly.
    """
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    out = []
    for r in rows:
        out.append(
            {
                cols[i]: (str(r[i]) if r[i] is not None else None)
                for i in range(len(cols))
            }
        )
    return out


with DAG(
    dag_id="store_transactions_ingest",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["store", "transactions"],
    template_searchpath=["/opt/airflow/sql_files"],
) as dag:

    wait_for_raw_file = FileSensor(
        task_id="wait_for_raw_file",
        filepath="/opt/airflow/store_files/raw/raw_store_transactions.csv",
        fs_conn_id="fs_default",
        poke_interval=15,
        timeout=60 * 30,
        mode="poke",
    )

    clean_file = PythonOperator(
        task_id="data_cleaner",
        python_callable=data_cleaner,
        op_kwargs={
            "input_path": "/opt/airflow/store_files/raw/raw_store_transactions.csv",  # noqa: E501
            "output_path": "/opt/airflow/store_files/cleaned/clean_store_transactions.csv",  # noqa: E501
        },
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_store_transactions_table",
        conn_id="airflow_postgres",
        sql="create_store_transactions.sql",
    )

    load_table = SQLExecuteQueryOperator(
        task_id="load_store_transactions",
        conn_id="airflow_postgres",
        sql="load_store_transactions.sql",
    )

    # --- REPORT QUERIES (XCom output as list[dict]) ---

    location_wise_profit = SQLExecuteQueryOperator(
        task_id="location_wise_profit_report",
        conn_id="airflow_postgres",
        sql="location_wise_profit_report.sql",
        handler=fetch_dicts,
        do_xcom_push=True,
    )

    store_wise_profit = SQLExecuteQueryOperator(
        task_id="store_wise_profit_report",
        conn_id="airflow_postgres",
        sql="store_wise_profit_report.sql",
        handler=fetch_dicts,
        do_xcom_push=True,
    )

    email_analysts = EmailOperator(
        task_id="email_analysts_reports_ready",
        to=["analysts@example.com"],  # replace
        subject="Store profit report – {{ ds }}",
        html_content="""
        <p>Hi Analysts,</p>

        <p>Below are the profit summaries for the <b>latest available transaction date</b>.</p> 

        <h3>📍 Location-wise Profit</h3>
        <table border="1" cellpadding="4" cellspacing="0">
          <tr>
            <th>Date</th>
            <th>Store Location</th>
            <th>Profit</th>
          </tr>
          {% for row in ti.xcom_pull(task_ids='location_wise_profit_report') %}
          <tr>
            <td>{{ row["transaction_date"] }}</td>
            <td>{{ row["store_location"] }}</td>
            <td>{{ row["lc_profit"] }}</td>
          </tr>
          {% endfor %}
        </table>

        <br/>

        <h3>🏬 Store-wise Profit</h3>
        <table border="1" cellpadding="4" cellspacing="0">
          <tr>
            <th>Date</th>
            <th>Store ID</th>
            <th>Profit</th>
          </tr>
          {% for row in ti.xcom_pull(task_ids='store_wise_profit_report') %}
          <tr>
            <td>{{ row["transaction_date"] }}</td>
            <td>{{ row["store_id"] }}</td>
            <td>{{ row["st_profit"] }}</td>
          </tr>
          {% endfor %}
        </table>

        <p><b>Profit formula:</b> <code>SUM(SP) - SUM(CP)</code></p>

        <p>Regards,<br/>Airflow</p>
        """,  # noqa: E501
    )

    wait_for_raw_file >> clean_file >> create_table >> load_table
    load_table >> [location_wise_profit, store_wise_profit] >> email_analysts
