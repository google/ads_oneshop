# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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


def format_model_results(
    model_results: pd.DataFrame,
    significant_threshold: float = 0.10
) -> pd.DataFrame:
  """Formats the model results and applies a priority layer.

  Args:
    model_results: Dataframe for model results that contains effects and
      p_values.
    significant_threshold: Threshold for statistical significance.

  Returns:
    model_output: Dataframe that contains priority level for each MEX metric.
  """

  model_results = model_results.drop(
      model_results[model_results['mex_metric'] == 'Intercept'].index
  )
  model_results['significant'] = np.where(
      model_results['p_values'] <= significant_threshold, True, False
  )
  model_results['effects_guardrail'] = np.where(
      model_results['significant'], model_results['effects'], 0
  )

  # priority mapping layer
  priority_layer = pd.qcut(
      model_results['effects_guardrail'],
      q=[0, 0.25, 0.75, 1],
      labels=['Low', 'Medium', 'High'],
  )
  priority_layer.name = 'priority'

  model_output = pd.concat([model_results, priority_layer], axis=1).reset_index(
      drop=True
  )

  return model_output
