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

"""Tests for Merchant Excellence model."""

import unittest

import pandas as pd

import model


class TestModel(unittest.TestCase):

  def test_prepare_raw_data_correct(self):
    input_data = pd.DataFrame(
        data=[
            [5, 100, True, False, True],
            [10, 100, True, True, False],
            [15, 100, True, True, True],
            [20, 'null', True, True, True],
            [30, '0', True, True, True],
            [40, 0, True, True, True],
        ],
        columns=['clicks_30days', 'impressions_30days', 'mex1', 'mex2', 'mex3'],
    )
    expected_result = pd.DataFrame(
        data=[
            [0.05, 1, 0, 1],
            [0.10, 1, 1, 0],
            [0.15, 1, 1, 1],
        ],
        columns=[
            'ctr',
            'mex1',
            'mex2',
            'mex3'
        ],
    )
    result = model.prepare_raw_data(raw_data=input_data, raw_metrics=[
        'clicks_30days',
        'impressions_30days',
        'mex1',
        'mex2',
        'mex3'])
    pd.testing.assert_frame_equal(result, expected_result)

  def test_raise_error_if_unknown_metric(self):
    input_data = pd.DataFrame(
        data=[
            [1, 200, 'FALSE', 'FALSE', 'FALSE'],
        ],
        columns=[
            'clicks_30days',
            'impressions_30days',
            'mex1',
            'mex2',
            'mex3'
        ],
    )
    with self.assertRaises(ValueError):
      model.prepare_raw_data(
          raw_data=input_data,
          raw_metrics=['clicks_30days', 'impressions_30days', 'unknown_metric']
      )

  def test_raise_error_if_missing_performance_metric(self):
    input_data = pd.DataFrame(
        data=[
            [1, 200, 'FALSE', 'FALSE', 'FALSE'],
        ],
        columns=[
            'clicks_30days',
            'impressions_30days',
            'mex1',
            'mex2',
            'mex3'
        ],
    )
    with self.assertRaises(ValueError):
      model.prepare_raw_data(
          raw_data=input_data,
          raw_metrics=['impressions_30days', 'mex1', 'mex2', 'mex3']
      )

  def test_run_model_correct(self):
    input_data = pd.DataFrame(
        data=[
            ['shopping1', 0.06, 1, 0, 0],
            ['shopping2', 0.07, 1, 0, 0],
            ['shopping3', 0.08, 1, 0, 0],
            ['shopping4', 0.09, 1, 0, 0],
            ['shopping5', 0.04, 0, 1, 0],
            ['shopping6', 0.03, 0, 1, 0],
            ['shopping7', 0.02, 0, 1, 0],
            ['shopping8', 0.01, 0, 0, 1],
            ['shopping9', 0.01, 0, 0, 1],
            ['shopping10', 0.00, 0, 0, 1],
        ],
        columns=['offer', 'ctr', 'mex1', 'mex2', 'mex3'],
    )

    expected_result = pd.DataFrame(
        data=[
          ['mex1', 0.003033, 0],
          ['mex2', 0.002083, 0],
          ['mex3', 0.012829, 0],
        ],
        columns=[
            'mex_metric',
            'effects',
            'p_values',
        ],
    )

    result = model.run_model(
        model_data=input_data,
        dependent_var='ctr',
        explanatory_var=['mex1', 'mex2', 'mex3'],
    )

    pd.testing.assert_frame_equal(
        result, expected_result, atol=0.5e-1, rtol=0.5e-1
    )

  def test_format_model_results_correct(self):
    input_data = pd.DataFrame(
        data=[
            ['mex1', 0.05],
            ['mex2', 0.01],
            ['mex3', 0.0001],
            ['mex4', 0],
            ['mex5', 0],
        ],
        columns=[
            'mex_metric',
            'effects',
        ],
    )

    expected_result = pd.DataFrame(
        data=[
            ['mex1', 0.05, True, 'High'],
            ['mex2', 0.01, True, 'High'],
            ['mex3', 0.0001, True, 'Medium'],
            ['mex4', 0, False, 'Low'],
            ['mex5', 0, False, 'Low'],
        ],
        columns=[
            'mex_metric',
            'effects',
            'significant',
            'priority'
        ],
    )

    result = model.format_model_results(
        model_results=input_data,
        significant_threshold=0,
        priority_setting=0.4999
    )

    pd.testing.assert_frame_equal(result, expected_result)


if __name__ == "__main__":
    unittest.main()
