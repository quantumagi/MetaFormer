from celery import shared_task
from data_processor.utils.background_inference_task import BackgroundInferenceTask
from celery.signals import task_prerun, task_postrun
import logging

logger = logging.getLogger(__name__)

@shared_task(name="process_file_task")
def process_file_task(for_user, dataset_name, schema_str):
    try:
        BackgroundInferenceTask.do_work(for_user, dataset_name, schema_str)
    except Exception as e:
        logger.error(f"Error processing file task: {str(e)}")
        raise e

@task_prerun.connect
def task_prerun_handler(sender=None, headers=None, body=None, **kwargs):
    logger.info(f'Task {sender} starting...')

@task_postrun.connect
def task_postrun_handler(sender=None, headers=None, body=None, **kwargs):
    logger.info(f'Task {sender} finished...')
