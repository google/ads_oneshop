SELECT extraction_date,
    merchant_id,
    aggregator_id,
    channel,
    targeted_country,
    product_type_lvl1,
    product_type_lvl2,
    product_type_lvl3,
    custom_label_0,
    custom_label_1,
    custom_label_2,
    custom_label_3,
    custom_label_4,
    brand,
    metric_name,
    benchmark,
    comparison_type,
    metric_level,
    description,
    support_link,
    metric_category,
    priority,
    lia_metric,
    metric_value,
    total_products
FROM ${PROJECT_NAME}.${DATASET_NAME}.MEX_All_Metrics
