import logging
from datetime import datetime

from airflow import models
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators import ssh_operator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models.variable import Variable
from airflow.operators import python_operator
from lib.get_dates import get_date

# Common methods
from lib.slack_msg import slack_msg_body

# DEFINE INIT PARAMS
# Dag
dag_name = "legacy_data-sac_customer_attention_service_metrics"
dag_tags = [
    "production",
    "ETL",
    "schedule",
    "dockerhost",
    "legacy",
    "git: legacy/data-sac",
    "input: pending",
    "output: pending",
]
# Docker image
docker_image = "gcr.io/data-poc-323413/legacy/customer-attention-service-metrics:latest"
# Schedule interal
schedule_interval = "0 * * * *"
# Slack msg
riskiness = "Medium"
utility = """This pipeline makes available data associated with customer service
performed by the SAC team. From this data you can obtain information
associated with reasons for requesting attention, date of opening and
resolution of the attention case, attention channel, results of satisfaction surveys,
among other data more associated with the requesting client. This data is obtained
through the interaction with various endpoints available on the
Zendesk and Surveypal platforms and is recorded in our datawarehouse
after being transformed into this pipeline."""
sshHook = SSHHook(ssh_conn_id="ssh_public_pentaho")
connect_dockerhost = Variable.get("CONNECT_DOCKERHOST")
SLACK_CONN_ID = "slack"


default_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime(2022, 8, 3),
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
        command_line = f"""--rm -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                -v /home/bnbiuser/secrets/secrets_zendesk:/app/zendesk-api-secret \
                -v /home/bnbiuser/secrets/secrets_surveypal:/app/surveypal-api-secret \
                -e APP_DW_SECRET=/app/db-secret \
                -e APP_ZENDESK_API_SECRET=/app/zendesk-api-secret \
                -e APP_SURVEYPAL_API_SECRET=/app/surveypal-api-secret \
                {docker_image} \
                -email_from='noreply@yapo.cl' \
                -email_to='data_team@adevinta.com'"""
        call = ssh_operator.SSHOperator(
            task_id="task_customer_attention_service_metrics",
            ssh_hook=sshHook,
            command=f"""{connect_dockerhost} <<EOF \n
                        docker pull {docker_image} \n
                        sudo docker run {command_line}""",
        )
        call.execute(context=kwargs)

    customer_attention_service_metrics = python_operator.PythonOperator(
        task_id="task_customer_attention_service_metrics",
        provide_context=True,
        python_callable=call_ssh,
    )

    customer_attention_service_metrics
