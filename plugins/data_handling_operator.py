import logging
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)

class DataHandlingOperator(BaseOperator):

    @apply_defaults
    def __init__(self, param, *args, **kwargs):
        self.operator_param = param
        super(DataHandlingOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Hello world again")
        print("HELLLLLLLLLLLLLLLLLLLLLLLLLLLLLO")
        print("my param: ", self.operator_param)
        log.info('operator_param: %s', self.operator_param)




class DataHandlingPlugin(AirflowPlugin):
    name = "data_handling_plugin"
    operators = [DataHandlingOperator]