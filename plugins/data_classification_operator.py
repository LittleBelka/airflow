from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)

class DataClassificationOperator(BaseOperator):

    @apply_defaults
    def __init__(self, param, *args, **kwargs):
        self.operator_param = param
        super(DataClassificationOperator, self).__init__(*args, **kwargs)


    def execute(self, context):
        log.info("Start data classification.")


class DataClassificationPlugin(AirflowPlugin):
    name = "data_classification_plugin"
    operators = [DataClassificationOperator]