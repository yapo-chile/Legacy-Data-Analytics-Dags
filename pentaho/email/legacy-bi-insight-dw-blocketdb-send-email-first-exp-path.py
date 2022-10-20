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
dag_name = "legacy_bi-insight-dw-blocketdb_send_email_first_exp_path"
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
schedule_interval = "30 7 * * *"
# Slack msg
riskiness = "Medium"  # High, Medium or Low
utility = "Legacy etl related to Send Email first exp path"

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


def set_dates(**kwargs):
    dates = get_date(**kwargs)
    kwargs["ti"].xcom_push(key="start_date", value=dates["start_date"])
    kwargs["ti"].xcom_push(key="end_date", value=dates["end_date"])
    logging.info(f"start date to execute process: {dates['start_date']}")
    logging.info(f"end date to execute process: {dates['end_date']}")


with models.DAG(
    dag_name,
    tags=dag_tags,
    schedule_interval=schedule_interval,
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    on_failure_callback=task_fail_slack_alert,
) as dag:
    set_dates_email_first_exp_path = python_operator.PythonOperator(
        task_id="task_set_dates_email_first_exp_path",
        provide_context=True,
        python_callable=set_dates,
    )

    send_email_first_exp_path = SSHOperator(
        task_id="task_run_send_email_first_exp_path",
        ssh_hook=sshHook,
        command='sh /opt/dw_schibsted/yapo_bi/dw_blocketdb/send_email_first_exp_patch/run_jb_send_email_first_exp_patch.sh -date1="{{ ti.xcom_pull(task_ids="task_set_dates_email_first_exp_path", key="start_date") }}" -date2="{{ ti.xcom_pull(task_ids="task_set_dates_email_first_exp_path", key="end_date") }}" ',  # You need add a space at the end of the command, to avoid error: Jinja template not found
    )

    set_dates_email_first_exp_path >> send_email_first_exp_path
