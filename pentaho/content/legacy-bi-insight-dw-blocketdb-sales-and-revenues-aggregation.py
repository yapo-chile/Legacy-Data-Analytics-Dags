import logging
from datetime import datetime

from airflow import models
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from lib.get_dates import get_date

# Common methods
from lib.slack_msg import slack_msg_body

# DEFINE INIT PARAMS
# Dag
dag_name = "legacy_bi-insight-dw-blocketdb_sales_and_revenues_aggregation"
dag_tags = [
    "production",
    "ETL",
    "schedule",
    "pentaho",
    "git: legacy/bi-insight",
    "input: pending",
    "output: pending",
    "legacy",
]
# Schedule interal
schedule_interval = "30 9 * * *"
# Slack msg
riskiness = "Medium"  # High, Medium or Low
utility = "Legacy etl related to Sales And Revenues Aggregation"

SLACK_CONN_ID = "slack"
sshHook = SSHHook(ssh_conn_id="ssh_public_pentaho")


default_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime(2022, 7, 14),
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


with models.DAG(
    dag_name,
    tags=dag_tags,
    schedule_interval=schedule_interval,
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    on_failure_callback=task_fail_slack_alert,
) as dag:

    run_sales_and_revenues_aggregation = SSHOperator(
        task_id="task_run_sales_and_revenues_aggregation",
        ssh_hook=sshHook,
        command="sh /opt/dw_schibsted/yapo_bi/dw_blocketdb/finance/run_etl_sales_revenues.sh ",  # You need add a space at the end of the command, to avoid error: Jinja template not found
    )

    trigger_dag_composer_pipeline_tableau_marketing_kpis = TriggerDagRunOperator(
        task_id="task_trigger_dag_composer_pipeline_tableau_marketing_kpis",
        trigger_dag_id="composer_pipeline_tableau_marketing_kpis",
        wait_for_completion=True,
    )

    (
        run_sales_and_revenues_aggregation
        >> trigger_dag_composer_pipeline_tableau_marketing_kpis
    )
