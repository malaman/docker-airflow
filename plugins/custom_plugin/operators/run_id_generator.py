import datetime

from airflow.models import BaseOperator


class RunIdGenerator(BaseOperator):
    def custom_run_id(ts: str) -> str:
        """
        :param ts: in iso format (i.e. 2019-10-28T14:09:23.201089+00:00)
        :return: numeric representation of timestamp (i.e. 20191028140923)
        """
        date_time_obj = datetime.datetime.fromisoformat(ts)
        return date_time_obj.strftime("%Y%m%d%H%M%S")
