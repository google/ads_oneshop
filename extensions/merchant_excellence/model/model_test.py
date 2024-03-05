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


if __name__ == "__main__":
    unittest.main()
