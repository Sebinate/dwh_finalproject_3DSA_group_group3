import os
import pandas as pd
import numpy as np

from scripts.utils.utils import connect
from scripts.utils import ml_utils
from dotenv import load_dotenv
from pmdarima import auto_arima

class Processing():
    def __init__(self, ver_num: str, cutoff: str):
        self.version = f"v{ver_num}"
        self.artifact_dir = os.path.join("artifacts/", self.version)
        os.makedirs(self.artifact_dir, exist_ok=True)

        self.engine = connect()

        data_query = f"""SELECT * FROM analytics.ML_view WHERE monthly_date < {cutoff}"""
        self.data: pd.DataFrame = pd.read_sql(data_query, con = self.engine)

    def pivot_data(self):
        if not isinstance(self.data['date'], pd.DatetimeIndex):
            self.data["date"] = pd.to_datetime(self.data["date"])
        
        self.data = self.data.pivot_table(index = "date", 
                                      columns = "user_type", 
                                      values = [
                                          "order_counts",
                                          "revenue",
                                          "average_delays",
                                          "average_discount"
                                      ],
                                      aggfunc = {
                                          "order_counts": "sum",
                                          "revenue": "sum",
                                          "delays": "mean",
                                          "average_discount": "mean"
                                      }).fillna(0)

        self.data.columns = [f'{metric}_{data}' for metric, data in self.data.columns]

        ml_utils.to_np_arr(os.path.join(self.artifact_dir, f"trained_{self.cutoff}", "data.npz"), 
                           data = self.data, 
                           names = self.data.columns)

    def train(self):
        targets = ['revenue_basic', 'revenue_premium', 'revenue_verified']
        
        months_to_forecast = 6 

        future_dates = pd.date_range(
            start=self.data.index[-1] + pd.Timedelta(days=1), 
            periods=months_to_forecast, 
            freq='MS'
        )
        self.forecast_df = pd.DataFrame(index=future_dates)

        for col in targets:
            if os.path.exists(os.path.join(self.artifact_dir, "models", f"ARIMA_{col}.pkl")):
                continue
            series = self.data[col]
            model = auto_arima(
                series,
                start_p=1, start_q=1,
                max_p=3, max_q=3,
                m=12,
                seasonal=True,
                d=None,
                D=1,
                trace=False,
                error_action='ignore',  
                suppress_warnings=True, 
                stepwise=True
            )
            #predicting part
            preds = model.predict(n_periods=months_to_forecast)
            self.forecast_df[col] = preds.clip(lower=0)
            ml_utils.to_pkl(os.path.join(self.artifact_dir, "models"), f"ARIMA_{col}", model)

        self.forecast_df.to_sql("forecast_revenue_6_months",
                                con = self.engine, 
                                schema = "analytics", 
                                if_exists = "replace")
        
    def run(self):
        self.pivot_data()
        self.train()

ML_process = Processing("1.0.0", "2024-01-01")
ML_process.run()

