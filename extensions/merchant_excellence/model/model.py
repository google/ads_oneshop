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

"""Model for Merchant Excellence Solution."""

from absl import app
from absl import flags
from typing import Sequence

import numpy as np
import pandas as pd

from sklearn import linear_model
import shap


_DATASET_NAME = flags.DEFINE_string(name='dataset_name', default=None, help='The dataset name.', required=True)
_PROJECT_NAME = flags.DEFINE_string(name='project_name', default=None, help='The project name.', required=True)


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
    model_data[metric] = np.where((raw_data[metric] == True), 1, 0)

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

  mex_cols = ['mex_metric', 'effects', 'p_values']
  X = model_data[explanatory_var]
  y = model_data[dependent_var]

  try:
    fit_model = linear_model.RidgeCV(
        alphas=[0.001, 0.01, 1, 10, 50, 100], cv=3
    ).fit(X, y)
    explainer = shap.LinearExplainer(
        model=fit_model, masker=shap.maskers.Independent(data=X)
    )
    shapley_values = explainer(X)
    model_results = pd.DataFrame(
        data={'abs_mean': shapley_values.abs.mean(0).values},
        index=X.columns,
    )
    model_results['p_values'] = 0 # temp before fixing format
    model_results.reset_index(inplace=True)
    model_results.columns = mex_cols
  except ValueError as e:
    print(f"Failed to run model: {e}")
    model_results = pd.DataFrame(columns=mex_cols)

  return model_results


def format_model_results(
    model_results: pd.DataFrame,
    significant_threshold: float = 0.0,
    priority_setting=0.4999,
) -> pd.DataFrame:
  """Formats the model results and applies a priority layer.

  Args:
    model_results: Dataframe for model results that contains effects of and
      Shapley values of each MEX metric.
    significant_threshold: Threshold for variable importance using Shapley
      values. Typically anything above 0 means it has meaningful impact on the
      metric.
    priority_setting: Parameter between 0 and 1 for splitting the medium and
      high priority buckets. Higher values mean more metrics will be placed into
      medium priority and lower values means more metrics will be placed into
      high priority.

  Returns:
    model_output: Dataframe that contains priority level for each MEX metric.
  """

  model_results['significant'] = np.where(
      model_results['effects'] > significant_threshold, True, False
  )

  significant_metrics = model_results[
      model_results['significant'] == True
  ].copy()
  significant_metrics['priority'] = pd.qcut(
      significant_metrics['effects'],
      q=[0, priority_setting, 1],
      labels=['Medium', 'High'],
  )

  formatted_results = model_results.merge(
      significant_metrics[['mex_metric', 'priority']],
      on=['mex_metric'],
      how='left',
  )

  formatted_results['priority'] = np.where(
      formatted_results['significant'] == False,
      'Low',
      formatted_results['priority'],
  )

  return formatted_results


def main(argv: Sequence[str]):
  del argv
  sql = """SELECT * FROM `{}.MEX_ML_Data`""".format(_DATASET_NAME.value)
  df = pd.read_gbq(sql, dialect="standard")
  raw_metrics = list(df.columns)
  model_data = prepare_raw_data(df, raw_metrics)
  model_results = run_model(
      model_data,
      'ctr',
      [
          'has_dynamic_remarketing',
          'has_free_listings',
          'has_price_availability_aiu',
          'has_item_level_shipping',
          'has_account_level_shipping',
          'has_image_aiu',
          'has_availability_aiu',
          'has_custom_label',
          'has_item_group_id',
          'has_good_product_type',
          'has_brand',
          'has_gtin',
          'has_500_description',
          'has_30_title',
          'has_product_highlight',
          'has_color',
          'has_age_group',
          'has_gender',
          'has_size',
          'has_mhlsf_implemented',
          'has_store_pickup_implemented',
          'has_odo_implemented',
          'has_sale_price',
          'has_additional_images',
      ],
  )
  model_output = format_model_results(model_results, 1.0)
  table_id = _DATASET_NAME.value + ".ML_Model_Output"
  model_output.to_gbq(table_id, project_id=_PROJECT_NAME.value, if_exists='replace')

if __name__ == '__main__':
  app.run(main)
