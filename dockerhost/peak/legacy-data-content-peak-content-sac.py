from __future__ import print_function

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
dag_name = "legacy_data-content_peak-content-sac"
dag_tags = [
    "production",
    "ETL",
    "schedule",
    "dockerhost",
    "legacy",
    "git: legacy/data-content",
    "input: dwh",
    "input: zendesk",
    "input: surveypal API",
    "output: dwh",
]
# Docker image
docker_image = "registry.gitlab.com/yapo_team/legacy/data-analytics/data-content:71ae4cac_peak-content-sac"
# Schedule interal
schedule_interval = "0 * * * *"
# Slack msg
riskiness = "Medium"
utility = "This pipeline makes available data associated with customer service performed by the SAC team."
# Other information:
"""From this data you can obtain information associated with reasons for requesting attention, date of opening and
resolution of the attention case, attention channel, results of satisfaction surveys, among other data more associated
with the requesting client. This data is obtained through the interaction with various endpoints available on the
Zendesk and Surveypal platforms and is recorded in our datawarehouse after being transformed into this pipeline

Finally, it is important to mention that regarding the execution and re-execution strategy,
it is not necessary in this case to pass variables "from" "to" (dates vars) because these are obtained
by consulting the last data recorded in the output data tables at the DWH. That said,
to re-execute a time range and obtain this data again, it is necessary to delete this range in the aforementioned
output tables and this pipeline will automatically obtain the data for the time range in question, up to the current day.
"""

sshHook = SSHHook(ssh_conn_id="ssh_public_pentaho")
connect_dockerhost = Variable.get("CONNECT_DOCKERHOST")
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


with models.DAG(
    dag_name,
    tags=dag_tags,
    schedule_interval=schedule_interval,
    default_args=default_args,
    max_active_runs=1,
    on_failure_callback=task_fail_slack_alert,
) as dag:

    def call_ssh(**kwargs):
        dates = get_date(**kwargs)
        logging.info(f"detected days: {dates}")
        command_line = f"""--rm -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                            -v /home/bnbiuser/secrets/secrets_zendesk:/app/zendesk-api-secret \
                            -v /home/bnbiuser/secrets/secrets_surveypal:/app/surveypal-api-secret \
                            -e APP_DW_SECRET=/app/db-secret \
                            -e APP_ZENDESK_API_SECRET=/app/zendesk-api-secret \
                            -e APP_SURVEYPAL_API_SECRET=/app/surveypal-api-secret \
                            {docker_image}"""
        call = ssh_operator.SSHOperator(
            task_id="task_run_peak_content_sac",
            ssh_hook=sshHook,
            command=f"""{connect_dockerhost} <<EOF \n
                        sudo docker pull {docker_image} \n
                        sudo docker run {command_line}""",
        )
        call.execute(context=kwargs)

    run_peak_content_sac = python_operator.PythonOperator(
        task_id="task_run_peak_content_sac",
        provide_context=True,
        python_callable=call_ssh,
    )

    run_peak_content_sac
