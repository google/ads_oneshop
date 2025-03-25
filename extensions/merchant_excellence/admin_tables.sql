-- Copyright 2024 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE IF NOT EXISTS ${PROJECT_NAME}.${DATASET_NAME}.liasettings (
    settings STRUCT<
        account_id INT64,
        country_settings ARRAY<
            STRUCT<
                country STRING,
                inventory STRUCT<
                    status STRING,
                    inventory_verification_contact_name STRING,
                    inventory_verification_contact_email STRING,
                    inventory_verification_contact_status STRING
                >,
                on_display_to_order STRUCT<
                    status STRING,
                    shipping_cost_policy_url STRING
                >,
                hosted_local_storefront_active BOOL,
                store_pickup_active BOOL,
                about STRUCT<
                    status STRING,
                    url STRING
                >,
                pos_data_provider STRUCT<
                    pos_data_provider_id INT64,
                    pos_external_account_id STRING
                >,
                omnichannel_experience STRUCT<
                    country STRING,
                    lsf_type STRING,
                    pickup_types ARRAY<STRING>
                >
            >
        >,
        kind STRING
    >,
    children ARRAY<
        STRUCT<
            account_id INT64,
            country_settings ARRAY<
                STRUCT<
                    country STRING,
                    inventory STRUCT<
                        status STRING,
                        inventory_verification_contact_name STRING,
                        inventory_verification_contact_email STRING,
                        inventory_verification_contact_status STRING
                    >,
                    on_display_to_order STRUCT<
                        status STRING,
                        shipping_cost_policy_url STRING
                    >,
                    hosted_local_storefront_active BOOL,
                    store_pickup_active BOOL,
                    about STRUCT<
                        status STRING,
                        url STRING
                    >,
                    pos_data_provider STRUCT<
                        pos_data_provider_id INT64,
                        pos_external_account_id STRING
                    >,
                    omnichannel_experience STRUCT<
                        country STRING,
                        lsf_type STRING,
                        pickup_types ARRAY<STRING>
                    >
                >
            >,
            kind STRING
        >
    >
);

CREATE TABLE IF NOT EXISTS ${PROJECT_NAME}.${DATASET_NAME}.shippingsettings (
    children ARRAY<
        STRUCT<
            settings STRUCT<
                accountId INT64,
                services ARRAY<
                    STRUCT<
                        deliveryTime STRUCT<
                            handlingBusinessDayConfig STRUCT<businessDays ARRAY<STRING>>,
                            maxTransitTimeInDays INT64,
                            minTransitTimeInDays INT64,
                            maxHandlingTimeInDays INT64,
                            minHandlingTimeInDays INT64,
                            cutoffTime STRUCT<timezone STRING, minute INT64, hour INT64>
                        >,
                        rateGroups ARRAY<
                            STRUCT<
                                applicableShippingLabels ARRAY<STRING>,
                                name STRING,
                                mainTable STRUCT<
                                    name STRING,
                                    `rows` ARRAY<
                                        STRUCT<
                                            cells ARRAY<
                                                STRUCT<
                                                    flatRate STRUCT<currency STRING, value FLOAT64>
                                                >
                                            >
                                        >
                                    >,
                                    rowHeaders STRUCT<
                                        prices ARRAY<
                                            STRUCT<currency STRING, value FLOAT64>
                                        >
                                    >,
                                    columnHeaders STRUCT<
                                        prices ARRAY<
                                            STRUCT<currency STRING, value FLOAT64>
                                        >
                                    >
                                >,
                                singleValue STRUCT<
                                    flatRate STRUCT<currency STRING, value FLOAT64>
                                >
                            >
                        >,
                        eligibility STRING,
                        shipmentType STRING,
                        currency STRING,
                        deliveryCountry STRING,
                        active BOOL,
                        name STRING
                    >
                >
            >
        >
    >,
    settings STRUCT<
        accountId INT64,
        services ARRAY<
            STRUCT<
                deliveryTime STRUCT<
                    handlingBusinessDayConfig STRUCT<businessDays ARRAY<STRING>>,
                    maxTransitTimeInDays INT64,
                    minTransitTimeInDays INT64,
                    maxHandlingTimeInDays INT64,
                    minHandlingTimeInDays INT64,
                    cutoffTime STRUCT<timezone STRING, minute INT64, hour INT64>
                >,
                rateGroups ARRAY<
                    STRUCT<
                        applicableShippingLabels ARRAY<STRING>,
                        name STRING,
                        mainTable STRUCT<
                            name STRING,
                            `rows` ARRAY<
                                STRUCT<
                                    cells ARRAY<
                                        STRUCT<
                                            flatRate STRUCT<currency STRING, value FLOAT64>
                                        >
                                    >
                                >
                            >,
                            rowHeaders STRUCT<
                                prices ARRAY<
                                    STRUCT<currency STRING, value FLOAT64>
                                >
                            >,
                            columnHeaders STRUCT<
                                prices ARRAY<
                                    STRUCT<currency STRING, value FLOAT64>
                                >
                            >
                        >,
                        singleValue STRUCT<
                            flatRate STRUCT<currency STRING, value FLOAT64>
                        >
                    >
                >,
                eligibility STRING,
                shipmentType STRING,
                currency STRING,
                deliveryCountry STRING,
                active BOOL,
                name STRING
            >
        >
    >
);