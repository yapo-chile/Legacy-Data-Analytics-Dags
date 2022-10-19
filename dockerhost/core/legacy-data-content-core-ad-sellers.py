from __future__ import print_function

import logging
from datetime import datetime

from airflow import models
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models.variable import Variable
from airflow.operators import python_operator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from lib.get_dates import get_date

# Common methods
from lib.slack_msg import slack_msg_body

# DEFINE INIT PARAMS
# Dag
dag_name = "legacy_data-content_ad_sellers"
dag_tags = [
    "production",
    "ETL",
    "dockerhost-pentaho",
    "legacy",
    "git: legacy/data-content",
    "input: pending",
    "output: pending",
]

# Schedule interal
schedule_interval = "10 4 * * *"
# Slack msg
riskiness = "Medium"
utility = "Daily this pipeline makes available into DWH the data related to sellers of ours ads whatever its classification private or professional as well as a detail available related to sellers pro in order to know in which categories a seller is professional"

SLACK_CONN_ID = "slack"


default_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime(2022, 7, 6),
}


def task_fail_slack_alert(context):
    slack_msg = slack_msg_body(
        context,
        riskiness=riskiness,
        utility=utility,
    )
    failed_alert = SlackWebhookOperator(
        task_id="failed_job_alert",
        http_conn_id=SLACK_CONN_ID,
        message=slack_msg,
        username="airflow",
        dag=dag,
    )
    return failed_alert.execute(context=context)


def set_dates(**kwargs):
    dates = get_date(**kwargs)
    kwargs["ti"].xcom_push(key="start_date", value=dates["start_date"])
    kwargs["ti"].xcom_push(key="end_date", value=dates["end_date"])


with models.DAG(
    dag_name,
    tags=dag_tags,
    schedule_interval=schedule_interval,
    default_args=default_args,
    max_active_runs=1,
    on_failure_callback=task_fail_slack_alert,
) as dag:

    set_dates_process = python_operator.PythonOperator(
        task_id="task_set_dates_ad_sellers",
        provide_context=True,
        python_callable=set_dates,
    )

    trigger_dag_legacy_trigger_data_content_ad_sellers = TriggerDagRunOperator(
        task_id="task_trigger_dag_legacy_trigger_data_content_ad_sellers",
        trigger_dag_id="legacy_trigger_data-content_ad_sellers",
        wait_for_completion=True,
        conf={
            "start_date": '{{ti.xcom_pull(task_ids="task_set_dates_ad_sellers", key="start_date")}}',
            "end_date": '{{ti.xcom_pull(task_ids="task_set_dates_ad_sellers", key="end_date")}}',
        },
    )

    trigger_dag_legacy_trigger_data_content_ads_created_daily = TriggerDagRunOperator(
        task_id="task_trigger_dag_legacy_trigger_data_content_ads_created_daily",
        trigger_dag_id="legacy_trigger_data-content_ads_created_daily",
        wait_for_completion=True,
        conf={
            "start_date": '{{ti.xcom_pull(task_ids="task_set_dates_ad_sellers", key="start_date")}}',
            "end_date": '{{ti.xcom_pull(task_ids="task_set_dates_ad_sellers", key="end_date")}}',
        },
    )
    trigger_dag_legacy_trigger_data_revenues_incremental_product_order = TriggerDagRunOperator(
        task_id="task_trigger_trigger_dag_legacy_trigger_data_revenues_incremental_product_order",
        trigger_dag_id="legacy_trigger_data-revenues_incremental_product_order",
        wait_for_completion=True,
        conf={
            "start_date": '{{ti.xcom_pull(task_ids="task_set_dates_ad_sellers", key="start_date")}}',
            "end_date": '{{ti.xcom_pull(task_ids="task_set_dates_ad_sellers", key="end_date")}}',
        },
    )
    trigger_dag_legacy_trigger_data_revenues_store_purshases = TriggerDagRunOperator(
        task_id="task_trigger_dag_legacy_trigger_data_revenues_store_purshases",
        trigger_dag_id="legacy_trigger_data-revenues_store-purshases",
        wait_for_completion=True,
        conf={
            "start_date": '{{ti.xcom_pull(task_ids="task_set_dates_ad_sellers", key="start_date")}}',
            "end_date": '{{ti.xcom_pull(task_ids="task_set_dates_ad_sellers", key="end_date")}}',
        },
    )
    trigger_dag_legacy_trigger_bi_insight_dw_blocketdb_etl_incremental_automatico = TriggerDagRunOperator(
        task_id="task_trigger_dag_legacy_trigger_bi_insight_dw_blocketdb_etl_incremental_automatico",
        wait_for_completion=True,
        trigger_dag_id="legacy_trigger_bi-insight-dw-blocketdb_etl_incremental_automatico",
        do_xcom_push=False,
    )

    (
        set_dates_process
        >> trigger_dag_legacy_trigger_data_content_ad_sellers
        >> trigger_dag_legacy_trigger_data_content_ads_created_daily
        >> trigger_dag_legacy_trigger_data_revenues_incremental_product_order
        >> trigger_dag_legacy_trigger_data_revenues_store_purshases
        >> trigger_dag_legacy_trigger_bi_insight_dw_blocketdb_etl_incremental_automatico
    )
