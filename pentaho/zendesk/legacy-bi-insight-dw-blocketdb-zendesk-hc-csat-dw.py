import logging
from datetime import datetime

from airflow import models
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators import python_operator
from lib.get_dates import get_date

# Common methods
from lib.slack_msg import slack_msg_body

# DEFINE INIT PARAMS
# Dag
dag_name = "legacy_bi-insight-dw_blocketdb_zendesk_hc_csat_dw"
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
schedule_interval = "0 10 * * *"  # This example scheduled at daily 6 AM: "0 6 * * *"
# Slack msg
riskiness = "Medium"  # High, Medium or Low
utility = "Legacy etl related to Zendesk automated moderation actions"

SLACK_CONN_ID = "slack"
sshHook = SSHHook(ssh_conn_id="ssh_public_pentaho")


default_args = {
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


with models.DAG(
    dag_name,
    tags=dag_tags,
    schedule_interval=schedule_interval,
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    on_failure_callback=task_fail_slack_alert,
) as dag:
    zendesk_hc_csat_dw = SSHOperator(
        task_id="task_zendesk_hc_csat_dw",
        do_xcom_push=False,
        ssh_hook=sshHook,
        command="sh /opt/dw_schibsted/yapo_bi/dw_blocketdb/zendesk_api/zd_integration/run_hc_csat_dw.sh ",
    )

    zendesk_hc_csat_dw
