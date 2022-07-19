import logging
from datetime import datetime

from airflow import models
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators import ssh_operator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models.variable import Variable
from airflow.operators import python_operator

# Common methods
from lib.slack_msg import slack_msg_body

# DEFINE INIT PARAMS
# Dag
dag_name = "legacy_data-insight_re_segmented_metrics"
dag_tags = [
    "production",
    "ETL",
    "schedule",
    "dockerhost",
    "legacy",
    "git: legacy/data-insight",
    "input: pending",
    "output: pending",
]
# Docker image
docker_image = "gcr.io/data-poc-323413/legacy/re_segmented_metrics:latest"
# Schedule interal
schedule_interval = "40 12 * * *"
# Slack msg
riskiness = "Medium"
utility = "ETL related insight re-segmented metrics"

sshHook = SSHHook(ssh_conn_id="ssh_public_pentaho")
connect_dockerhost = Variable.get("CONNECT_DOCKERHOST")

SLACK_CONN_ID = "slack"


default_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime(2022, 7, 19),
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
    on_failure_callback=task_fail_slack_alert,
) as dag:

    def call_ssh(**kwargs):
        command_line = f"""--rm --net=host \
                            -v /home/bnbiuser/secrets/pulse_auth:/app/pulse-secret \
                            -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                            -e APP_PULSE_SECRET=/app/pulse-secret \
                            -e APP_DB_SECRET=/app/db-secret \
                            {docker_image} \
                            -email_from='noreply@yapo.cl' \
                            -email_to='data_team@adevinta.com'"""
        call = ssh_operator.SSHOperator(
            task_id="task_re_segmented_metrics",
            ssh_hook=sshHook,
            command=f"""{connect_dockerhost} <<EOF \n
                        docker pull {docker_image} \n
                        sudo docker run {command_line}""",
        )
        call.execute(context=kwargs)

    re_segmented_metrics = python_operator.PythonOperator(
        task_id="task_re_segmented_metrics",
        provide_context=True,
        python_callable=call_ssh,
    )

    re_segmented_metrics