"""Library to run model for Merchant Excellence Solution."""

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf


def prepare_raw_data(
    raw_data: pd.DataFrame,
    raw_metrics: list[str],
) -> pd.DataFrame:
  """Prepares raw data for modeling ctr.

  Args:
    raw_data: Dataframe of raw data from MEX database.
    raw_metrics: List of raw metrics that includes clicks and impressions.

  Returns:
    model_data: Dataframe to be used for modeling.

  Raises:
    ValueError: If metric does not exist in raw_data.
  """

  for metric in raw_metrics:
    if metric not in raw_data.columns:
      raise ValueError(f'{metric} does not exist in raw data.')

  for performance_metric in ['clicks_30days', 'impressions_30days']:
    if performance_metric not in raw_metrics:
      raise ValueError(f'{performance_metric} needs to be included for ctr.')

  raw_data = raw_data[
      ~raw_data['impressions_30days'].isin(['null', '0', 0])
  ].reset_index()
  raw_data['clicks_30days'] = np.where(
      raw_data['clicks_30days'].isin(['null', '0']),
      0,
      raw_data['clicks_30days']
  )
  raw_data['ctr'] = raw_data['clicks_30days'].astype('int') / raw_data[
      'impressions_30days'].astype('int')

  model_data = pd.DataFrame()
  model_data['ctr'] = raw_data['ctr']
  mex_metrics = [
      metric for metric in raw_metrics
      if metric not in ['clicks_30days', 'impressions_30days']
  ]
  for metric in mex_metrics:
    model_data[metric] = np.where((raw_data[metric] == 'TRUE'), 1, 0)

  return model_data


def run_model(
    model_data: pd.DataFrame,
    dependent_var: str,
    explanatory_var: list[str],
) -> pd.DataFrame:
  """Runs ols model and returns the results.

  Args:
    model_data: Dataframe for Merchant Excellence model that contains a
      performance metric and MEX metrics.
    dependent_var: Performance metric to be used in the model.
    explanatory_var: MEX metrics to be used in the model.

  Returns:
    model_results: Dataframe that contains effects and p_values for each MEX
      metric.
  """

  formula = f'{dependent_var}~{" + ".join(explanatory_var)}'
  fit_model = smf.ols(formula=formula, data=model_data).fit()

  model_results = pd.concat([fit_model.params, fit_model.pvalues], axis=1)
  model_results.reset_index(inplace=True)
  model_results.columns = ['mex_metric', 'effects', 'p_values']

  return model_results
