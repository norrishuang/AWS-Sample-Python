from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable
import os

os.environ["AIRFLOW__OPENLINEAGE__DISABLED"] = "False"
os.environ["AIRFLOW__OPENLINEAGE__TRANSPORT"] = """{"type":"console"}"""

class EnvVarPlugin(AirflowPlugin):
    name = "env_var_plugin"