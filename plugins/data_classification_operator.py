from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import roc_auc_score, precision_score, recall_score, f1_score
from sklearn.metrics import precision_recall_curve, average_precision_score
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import pandas as pd

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

        # TODO: download train and test dataframes from mongodb
        train, test = self.download()

        neigh = KNeighborsClassifier(n_neighbors=3)
        y = train.Diagnosis
        X = train.drop('Diagnosis', 1)
        neigh.fit(X, y)

        test_x = test.drop('Diagnosis', 1)
        test_y = test.Diagnosis
        y_pred = neigh.predict(test_x)

        log.info("Calculate quality metrics.")

        # ROC AUC
        print("ROC AUC:", roc_auc_score(test_y, y_pred)) # 0.9692460317460316
        # Precision
        print("Precision:", precision_score(test_y, y_pred)) # 0.9726027397260274
        # Recall
        print("Recall:", recall_score(test_y, y_pred)) # 0.9861111111111112
        # F1 score
        print("F1 score:", f1_score(test_y, y_pred)) # 0.9793103448275863

        self.build_precision_recall_curve(test_y, y_pred)


    def build_precision_recall_curve(self, test_y, y_pred):
        average_precision = average_precision_score(test_y, y_pred)

        precision, recall, _ = precision_recall_curve(test_y, y_pred)

        plt.step(recall, precision, color='b', alpha=0.2,
                 where='post')
        plt.fill_between(recall, precision, step='post', alpha=0.2,
                         color='b')

        plt.xlabel('Recall')
        plt.ylabel('Precision')
        plt.ylim([0.0, 1.05])
        plt.xlim([0.0, 1.0])
        plt.title('2-class Precision-Recall curve: AP={0:0.2f}'.format(
            average_precision))

        plt.savefig('airflow/results/precision_recall_curve')
        plt.show()


    # Copipasted. Need to replace with mongodb usage
    def download(self):
        df = pd.read_csv('data/cancer.csv').set_index('ID')

        # Diagnosis: B = 0, M = 1
        df = pd.get_dummies(df)
        df = df.drop('Diagnosis_M', 1)
        df = df.rename(columns={'Diagnosis_B': 'Diagnosis'})

        train, test = train_test_split(df,
                                       test_size=0.2,
                                       random_state=213,
                                       stratify=df.Diagnosis)
        return train, test


class DataClassificationPlugin(AirflowPlugin):
    name = "data_classification_plugin"
    operators = [DataClassificationOperator]