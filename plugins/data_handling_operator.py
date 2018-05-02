import pandas as pd
import numpy as np
import logging
import json
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from pymongo import MongoClient

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)

class DataHandlingOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DataHandlingOperator, self).__init__(*args, **kwargs)


    def execute(self, context):
        log.info("Start data handling.")

        train, test = self.download()

        log.info("Data was splitted by train and test. Start to calculate the statistics.")
        self.calculate_statistics(train)
        # TODO: save statistics to mongodb

        log.info("Save data to the database.")
        self.insert_data_to_mongodb(train, test)


    def download(self):
        df = pd.read_csv('airflow/data/cancer.csv')

        # Diagnosis: B = 0, M = 1
        df = pd.get_dummies(df)
        df = df.drop('Diagnosis_M', 1)
        df = df.rename(columns={'Diagnosis_B': 'Diagnosis'})

        train, test = train_test_split(df,
                                       test_size=0.2,
                                       random_state=213,
                                       stratify=df.Diagnosis)
        return train, test


    def calculate_statistics(self, train):
        print('There are gaps in data: ', self.is_there_gaps_in_data(train))

        print('Data type in ID column:', train.ID.dtype)

        for name in train.columns[1:]:
            print('Data type in column:', train[name].dtype)

            print('Build histogram')
            self.build_histogram(train[name], name)

            print('Min and max values in column:')
            print(train[name].min(), ' ', train[name].max())

        print('Correlation with target value:')
        train.iloc[:, 1:].corr().Diagnosis

        print('Correlation with X1 column:')
        train.iloc[:, 1:].corr().X1

        print('Quantile 1:')
        train.iloc[:, 1:].quantile(.25)
        print('Quantile 2:')
        train.iloc[:, 1:].quantile(.5)
        print('Quantile 3:')
        train.iloc[:, 1:].quantile(.75)


    def build_histogram(self, x, title):
        plt.hist(x, bins=100)
        plt.title(title)
        plt.savefig('airflow/hist/' + title)
        plt.show()


    def is_there_gaps_in_data(self, train):
        nan_check = train.isna().any(axis=1).data
        nan_ar = np.array(nan_check)
        if True in nan_ar: return True
        return False


    def insert_data_to_mongodb(self, train, test):
        client = MongoClient()
        db = client.cancer

        if 'train' not in db.collection_names() or db.train.count() == 0:
            coll = db.train
            records = json.loads(train.T.to_json()).values()
            coll.insert_many(records)

        if 'test' not in db.collection_names() or db.test.count() == 0:
            coll = db.test
            records = json.loads(test.T.to_json()).values()
            coll.insert_many(records)


class DataHandlingPlugin(AirflowPlugin):
    name = "data_handling_plugin"
    operators = [DataHandlingOperator]