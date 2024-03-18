"""Tests for Merchant Excellence model."""

import unittest

import pandas as pd

import model


class TestModel(unittest.TestCase):

  def test_prepare_raw_data_correct(self):
    input_data = pd.DataFrame(
        data=[
            [5, 100, 'TRUE', 'FALSE', 'TRUE'],
            [10, 100, 'TRUE', 'TRUE', 'FALSE'],
            [15, 100, 'TRUE', 'TRUE', 'TRUE'],
            [20, 'null', 'TRUE', 'TRUE', 'TRUE'],
            [30, '0', 'TRUE', 'TRUE', 'TRUE'],
            [40, 0, 'TRUE', 'TRUE', 'TRUE'],
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
            ['Intercept', 0.027917, 0.000010],
            ['mex1', 0.047083, 0.000015],
            ['mex2', 0.002083, 0.686624],
            ['mex3', -0.021250, 0.00360],
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
            ['Intercept', 0.01, 0.001],
            ['mex1', 0.05, 0.0001],
            ['mex2', 0.01, 0.10],
            ['mex3', -1, 0.50],
            ['mex4', 0.025, 0.0001],
            ['mex5', -0.5, 0.10],
        ],
        columns=[
            'mex_metric',
            'effects',
            'p_values',
        ],
    )

    expected_result = pd.DataFrame(
        data=[
            ['mex1', 0.05, 0.0001, True, 0.05, 'High'],
            ['mex2', 0.01, 0.10, True, 0.01, 'Medium'],
            ['mex3', -1, 0.50, False, 0.0, 'Low'],
            ['mex4', 0.025, 0.0001, True, 0.025, 'Medium'],
            ['mex5', -0.5, 0.10, True, -0.5, 'Low'],
        ],
        columns=[
            'mex_metric',
            'effects',
            'p_values',
            'significant',
            'effects_guardrail',
            'priority'
        ],
    )
    expected_result['priority'] = pd.Categorical(
        expected_result['priority'],
        ordered=True,
        categories=['Low', 'Medium', 'High']
    )

    result = model.format_model_results(
        model_results=input_data,
        significant_threshold=0.10
    )

    pd.testing.assert_frame_equal(result, expected_result)


if __name__ == "__main__":
    unittest.main()
