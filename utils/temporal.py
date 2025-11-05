import pandas as pd

from utils.tasks import get_distinct_task_from_tasks_json


def calculate_temporal_coverage(distinct_field_values_for_this_model):
    origin_dates = distinct_field_values_for_this_model["origin_date"]
    horizons = distinct_field_values_for_this_model["horizon"]
    horizons = [int(horizon) for horizon in horizons]
    # Calculate temporal coverage, origin_date - 1 + horizon * 7
    temporal_coverage = {}
    for origin_date in origin_dates:
        ##get the max horizon for each origin_date
        max_horizon = max(horizons)
        ## convert origin_date to datetime object
        origin_date_datetime = pd.to_datetime(origin_date)
        ## calculate temporal coverage
        ## subtract 1 from origin_date
        startDate = origin_date_datetime

        ## add max_horizon * 7 to origin_date
        endDate = startDate - pd.DateOffset(days=1) + pd.DateOffset(weeks=max_horizon)
        temporal_coverage["startDate"] = startDate
        temporal_coverage["endDate"] = endDate

    return temporal_coverage
