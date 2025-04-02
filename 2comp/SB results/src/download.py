import warnings

warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

from google.cloud import bigquery

client = bigquery.Client(project='analytics-dev-333113')


def download_experiment_data(exp_id, user_name):
    tmp_query = f"""
    CREATE OR REPLACE TABLE `analytics-dev-333113.temp.{user_name}_exp`
    OPTIONS(
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    ) AS (
    WITH
    exp_list_prepare AS (
        SELECT
            id                            AS exp_id,
            name                          AS exp_name,
            status                        AS status, 
            start_datetime                AS utc_start_dttm,
            due_datetime                  AS utc_finish_dttm,
            `interval`                    AS switch_interval,
            salt                          AS exp_salt,
            conditions                    AS conditions,
            hexagon_size                  AS hexagon_size,
            hexagon_size > 0              AS is_hex,
            SAFE_CAST(
                CEILING(
                    TIMESTAMP_DIFF(
                        due_datetime, 
                        start_datetime, 
                        MINUTE
                    ) / `interval`
                ) AS INTEGER
            )                             AS intervals_cnt
        FROM `indriver-e6e40.ods_ab_platform.switchback`
        WHERE id = {exp_id}
    ),
    exp_order_type AS (
        SELECT
            exp_id,
            SAFE_CAST(order_type_id AS INTEGER) AS order_type_id
        FROM (
            SELECT
                exp_id,
                REGEXP_EXTRACT_ALL(conditions, r'"order_type_id"}}, "(\d+)"') AS order_type_ids
            FROM exp_list_prepare
        ) t,
        UNNEST(order_type_ids) AS order_type_id
    ),
    exp_city AS (
        SELECT
            exp_id,
            SAFE_CAST(REPLACE(city_id, '"', '') AS INTEGER) AS city_id
        FROM (
            SELECT
                switchback_id                                          AS exp_id,
                JSON_EXTRACT_ARRAY(conditions, '$.and[0].or[0].in[1]') AS city_ids
            FROM `indriver-e6e40.ods_ab_platform.switchback_region`
            WHERE switchback_id IN (SELECT exp_id FROM exp_list_prepare)
        ) t,
        UNNEST(city_ids) AS city_id
    ),
    exp_list AS (
        SELECT
            t1.exp_id,
            t1.exp_name,
            t1.status,
            t1.utc_start_dttm,
            t1.utc_finish_dttm,
            t2.order_type_id,
            t3.city_id,
            t1.switch_interval,
            t1.exp_salt,
            t1.hexagon_size,
            t1.is_hex,
            t1.intervals_cnt
        FROM exp_list_prepare t1
        LEFT JOIN exp_order_type t2
            ON t1.exp_id = t2.exp_id
        LEFT JOIN exp_city t3
            ON t1.exp_id = t3.exp_id
    ),
    hex_switches AS (
        SELECT DISTINCT
            exp_id,
            exp_name,
            status,
            utc_start_dttm,
            utc_finish_dttm,
            order_type_id,
            city_id,
            switch_interval,
            exp_salt,
            hexagon_size,
            is_hex,
            intervals_cnt,
            switch_start_dttm, 
            switch_finish_dttm,
            CAST(NULL AS STRING) AS group_name
        FROM (
            SELECT *,
                TIMESTAMP_ADD(utc_start_dttm, INTERVAL n * switch_interval MINUTE) AS switch_start_dttm,
                TIMESTAMP_ADD(utc_start_dttm, INTERVAL (n + 1) * switch_interval MINUTE) AS switch_finish_dttm
            FROM exp_list,
            UNNEST(GENERATE_ARRAY(0, intervals_cnt - 1)) AS n
            WHERE is_hex
        ) _
    ),
    no_hex_switches AS (
        SELECT DISTINCT
            exp_id,
            exp_name,
            status,
            utc_start_dttm,
            utc_finish_dttm,
            order_type_id,
            city_id,
            switch_interval,
            exp_salt,
            hexagon_size,
            is_hex,
            intervals_cnt,
            switch_start_dttm, 
            IFNULL(switch_finish_null, TIMESTAMP_ADD(switch_start_dttm, INTERVAL switch_interval MINUTE)) AS switch_finish_dttm,
            group_name
        FROM (
            SELECT
                t1.exp_id,
                t1.exp_name,
                t1.status,
                t1.utc_start_dttm,
                t1.utc_finish_dttm,
                t1.order_type_id,
                t1.city_id,
                t1.switch_interval,
                t1.exp_salt,
                t1.hexagon_size,
                t1.is_hex,
                t1.intervals_cnt,
                t2.start AS switch_start_dttm,
                LEAD(t2.start) OVER (PARTITION BY t1.exp_id, t1.city_id, t1.order_type_id ORDER BY t2.start ASC) AS switch_finish_null,
                t3.name AS group_name
            FROM exp_list t1
            LEFT JOIN `indriver-e6e40.ods_ab_platform.switchback_step` t2
                ON t1.exp_id = t2.switchback_id
            LEFT JOIN `indriver-e6e40.ods_ab_platform.group` t3
                ON t2.group_ids = TO_JSON_STRING(JSON_ARRAY(t3.id))
            WHERE true
                AND NOT t1.is_hex
        ) _ 
    ),
    together AS (
        SELECT *
        FROM hex_switches
        UNION ALL
        SELECT *
        FROM no_hex_switches
    )
    SELECT DISTINCT
        t1.*,
        t2.order_type,
        UNIX_SECONDS(t1.switch_start_dttm) AS switch_start_dttm_unix
    FROM together t1
    LEFT JOIN `analytics-dev-333113.me_reports.order_type_both_platforms` t2
        ON  t1.order_type_id = t2.order_type_id
        AND t1.city_id = t2.city_id
    ORDER BY exp_id, switch_start_dttm, switch_finish_dttm
    )
    """
    query = f"""
    SELECT *
    FROM `analytics-dev-333113.temp.{user_name}_exp`
    """
    client.query(tmp_query).result()
    return client.query(query).to_dataframe()


def download_order_data(start_date, stop_date, city_id, order_type, user_name):
    query = f"""
    WITH
    details_prepare AS (
        SELECT *
        FROM (
            SELECT
                city_id                                                                     AS city_id,
                order_type                                                                  AS order_type,
                order_uuid                                                                  AS order_uuid,
                user_id                                                                     AS user_id,
                order_timestamp                                                             AS local_order_dttm,
                TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_timestamp), timezone) AS utc_order_dttm,
                price_highrate_usd                                                          AS price_highrate_usd,
                price_start_usd                                                             AS price_start_usd,
                price_order_usd                                                             AS price_order_usd,
                tender_uuid                                                                 AS tender_uuid,
                driver_id                                                                   AS driver_id,
                price_tender_usd                                                            AS price_tender_usd,
                driveraccept_timestamp IS NOT NULL                                          AS is_order_accepted,
                driverdone_timestamp IS NOT NULL                                            AS is_order_done,
                tender_uuid IS NOT NULL                                                     AS is_order_with_tender,
                price_start_usd = price_tender_usd                                          AS is_order_start_price_bid,
                ROW_NUMBER() OVER (PARTITION BY order_uuid ORDER BY tender_timestamp ASC)   AS first_row_by_tender,
                fromlatitude                                                                AS fromlatitude,
                fromlongitude                                                               AS fromlongitude,
                duration_in_seconds / 60                                                    AS duration_in_min,
                distance_in_meters / 1000                                                   AS distance_in_km
            FROM `indriver-e6e40.emart.incity_detail`
            WHERE true
                AND created_date_order_part >= DATE_SUB(DATE('{start_date}'), INTERVAL 1 DAY)
                AND created_date_order_part <= DATE_ADD(DATE('{stop_date}'), INTERVAL 1 DAY)
                AND city_id = {city_id}
                AND order_type = '{order_type}'
        ) _
        WHERE DATE(utc_order_dttm) BETWEEN DATE('{start_date}') AND DATE('{stop_date}')
    ),
    orders_tbl AS (
        SELECT DISTINCT
            order_uuid,
            calcprice_uuid
        FROM (
            SELECT
                uuid                                                           AS order_uuid,
                MAX(price_calculation_uuid) OVER(PARTITION BY uuid)            AS calcprice_uuid,
                ROW_NUMBER() OVER(PARTITION BY uuid ORDER BY modified_at DESC) AS rn
            FROM `indriver-e6e40.ods_new_order_rh_cdc.order_global_strm`
            WHERE true
                AND DATE(created_at) >= DATE_SUB(DATE('{start_date}'), INTERVAL 1 DAY)
                AND DATE(created_at) <= DATE_ADD(DATE('{stop_date}'), INTERVAL 1 DAY)
                AND uuid IN (SELECT order_uuid FROM details_prepare)
        ) _
        WHERE rn = 1
    ),
    details_tbl AS (
        SELECT
            t1.city_id                            AS city_id,
            t1.order_type                         AS order_type,
            t1.order_uuid                         AS order_uuid,
            t1.local_order_dttm                   AS local_order_dttm,
            t1.utc_order_dttm                     AS utc_order_dttm,
            t1.price_highrate_usd                 AS price_highrate_usd,
            t1.price_start_usd                    AS price_start_usd,
            t1.fromlatitude                       AS fromlatitude,
            t1.fromlongitude                      AS fromlongitude,
            t1.duration_in_min                    AS duration_in_min,
            t1.distance_in_km                     AS distance_in_km,
            t2.tenders_count                      AS tenders_count,
            t2.price_tender_usd                   AS price_tender_usd,
            t2.is_order_with_tender               AS is_order_with_tender,
            t2.is_order_start_price_bid           AS is_order_start_price_bid,
            t2.is_order_accepted_start_price_bid  AS is_order_accepted_start_price_bid,
            t2.is_order_done_start_price_bid      AS is_order_done_start_price_bid,
            t2.is_order_accepted                  AS is_order_accepted,
            t2.is_order_done                      AS is_order_done,
            t3.price_done_usd                     AS price_done_usd,
            t3.rides_price_highrate_usd           AS rides_price_highrate_usd,
            t3.rides_price_start_usd              AS rides_price_start_usd,
            t4.calcprice_uuid                     AS calcprice_uuid
        FROM details_prepare t1
        LEFT JOIN (
            SELECT
                order_uuid,
                COUNT(DISTINCT tender_uuid)                         AS tenders_count,
                AVG(price_tender_usd)                               AS price_tender_usd,
                MAX(is_order_with_tender)                           AS is_order_with_tender,
                MAX(is_order_start_price_bid)                       AS is_order_start_price_bid,
                MAX(is_order_start_price_bid AND is_order_accepted) AS is_order_accepted_start_price_bid,
                MAX(is_order_start_price_bid AND is_order_done)     AS is_order_done_start_price_bid,
                MAX(is_order_accepted)                              AS is_order_accepted,
                MAX(is_order_done)                                  AS is_order_done
            FROM details_prepare
            GROUP BY 1
        ) t2
            ON t1.order_uuid = t2.order_uuid
        LEFT JOIN (
            SELECT
                order_uuid,
                AVG(price_order_usd)    AS price_done_usd,
                AVG(price_highrate_usd) AS rides_price_highrate_usd,
                AVG(price_start_usd)    AS rides_price_start_usd
            FROM details_prepare
            WHERE is_order_done
            GROUP BY 1
        ) t3
            ON t1.order_uuid = t3.order_uuid
        LEFT JOIN orders_tbl t4
            ON t1.order_uuid = t4.order_uuid
        WHERE first_row_by_tender = 1
    )
    SELECT 
        city_id,
        order_type,
        order_uuid,
        local_order_dttm,
        utc_order_dttm,
        price_highrate_usd,
        price_start_usd,
        fromlatitude,
        fromlongitude,
        duration_in_min,
        distance_in_km,
        tenders_count,
        price_tender_usd,
        is_order_with_tender,
        is_order_start_price_bid,
        is_order_accepted_start_price_bid,
        is_order_done_start_price_bid,
        is_order_accepted,
        is_order_done,
        price_done_usd,
        rides_price_highrate_usd,
        rides_price_start_usd,
        calcprice_uuid,
        switch_start_dttm, 
        switch_finish_dttm,
        hex_from,
        IF(
            mmhash IS NULL, 
            IF(group_name IS NULL, 'Before', group_name),
            IF(MOD(mmhash, 100) < 50, 'A','Control')
        ) AS order_group_name
    FROM (
        SELECT *,
            CAST(
                IF(
                    is_hex,
                    `indriver-e6e40.de_functions.murmurhash32`(exp_salt || switch_start_dttm_unix || hex_from),
                    NULL
                ) AS INTEGER
            ) AS mmhash
        FROM (
            SELECT 
                t1.*, t2.* EXCEPT(city_id, order_type),
                `indriver-e6e40.de_functions.h3_lat_lng_to_cell_temp`(
                    t1.fromlatitude, 
                    t1.fromlongitude, 
                    IF(t2.hexagon_size IS NULL, 7, t2.hexagon_size)
                ) AS hex_from
            FROM details_tbl t1
            LEFT JOIN `analytics-dev-333113.temp.{user_name}_exp` t2
                ON  t1.city_id = t2.city_id
                AND t1.utc_order_dttm >= t2.switch_start_dttm
                AND t1.utc_order_dttm < t2.switch_finish_dttm
            WHERE ((t1.order_type = t2.order_type) OR (t2.order_type IS NULL))
        ) _
    ) _
    """
    return client.query(query).to_dataframe()


def download_recprice_data(start_date, stop_date, city_id, order_type, user_name):
    query = f"""
    WITH
    recprice_tbl AS (
        SELECT *
        FROM (
            SELECT DISTINCT
                t1.city_id                                            AS city_id,
                t4.order_type                                         AS order_type,
                t1.id                                                 AS calcprice_uuid,
                t1.user_id                                            AS user_id,
                TIMESTAMP(DATETIME(t1.calculation_dttm, t2.timezone)) AS local_recprice_dttm,
                t1.calculation_dttm                                   AS utc_recprice_dttm,
                t1.base_price / t3.usd_value                          AS price_base_usd,
                t1.price / t3.usd_value                               AS recprice_usd,
                t1.min_price / t3.usd_value                           AS minprice_usd,
                t1.surge                                              AS surge,
                t1.dynamic_surge                                      AS dynamic_surge,
                t1.start_point_latitude                               AS fromlatitude,
                t1.start_point_longitude                              AS fromlongitude,
                t1.distance                                           AS log_distance_in_km,
                t1.duration / 60                                      AS log_duration_in_min,
                CAST(JSON_EXTRACT_SCALAR(etc, "$.two_component_surge.original_surge") AS FLOAT64) AS original_dynamic_surge,
            FROM `indriver-e6e40.ods_recprice_cdc.pricing_logs` t1
            LEFT JOIN `indriver-e6e40.ods_monolith.tbl_city` t2
                ON t1.city_id = t2.id
            LEFT JOIN `indriver-bi.heap.currency_by_date` t3
                ON  t2.country_id = t3.country_id
                AND DATE(t1.calculation_dttm) = t3.date
            LEFT JOIN `analytics-dev-333113.me_reports.order_type_both_platforms` t4
                ON  t1.city_id = t4.city_id
                AND t1.order_type_id = t4.order_type_id
            WHERE true
                AND DATE(t1.calculation_dttm) >= DATE_SUB(DATE('{start_date}'), INTERVAL 1 DAY)
                AND DATE(t1.calculation_dttm) <= DATE_ADD(DATE('{stop_date}'), INTERVAL 1 DAY)
                AND t1.city_id = {city_id}
                AND t4.order_type = '{order_type}'
                AND t1.user_id NOT IN (160705043, 10368574)
                AND (
                    t1.user_agent NOT IN ('recprice-load-generator','PHPMonolith/1','Python/3.8 aiohttp/3.8.4') OR 
                    t1.user_agent IS NULL
                )
        ) _
        WHERE DATE(utc_recprice_dttm) BETWEEN DATE('{start_date}') AND DATE('{stop_date}')
    )
    SELECT 
        city_id,
        order_type,
        calcprice_uuid,
        user_id,
        local_recprice_dttm,
        utc_recprice_dttm,
        price_base_usd,
        recprice_usd,
        minprice_usd,
        surge,
        dynamic_surge,
        fromlatitude,
        fromlongitude,
        log_distance_in_km,
        log_duration_in_min,
        original_dynamic_surge,
        CASE 
            WHEN original_dynamic_surge = 0 THEN dynamic_surge
            ELSE original_dynamic_surge
        END AS original_dynamic_surge_updated,
        switch_start_dttm, 
        switch_finish_dttm,
        hex_from,
        IF(
            mmhash IS NULL, 
            IF(group_name IS NULL, 'Before', group_name),
            IF(MOD(mmhash, 100) < 50, 'A','Control')
        ) AS recprice_group_name
    FROM (
        SELECT *,
            CAST(
                IF(
                    is_hex, 
                    `indriver-e6e40.de_functions.murmurhash32`(exp_salt || switch_start_dttm_unix || hex_from), 
                    NULL
                ) AS INTEGER
            ) AS mmhash
        FROM (
            SELECT 
                t1.*, t2.* EXCEPT(city_id, order_type),
                `indriver-e6e40.de_functions.h3_lat_lng_to_cell_temp`(
                    t1.fromlatitude, 
                    t1.fromlongitude, 
                    IF(t2.hexagon_size IS NULL, 7, t2.hexagon_size)
                ) AS hex_from
            FROM recprice_tbl t1
            LEFT JOIN `analytics-dev-333113.temp.{user_name}_exp` t2
                ON  t1.city_id = t2.city_id
                AND t1.utc_recprice_dttm >= t2.switch_start_dttm
                AND t1.utc_recprice_dttm < t2.switch_finish_dttm
            WHERE ((t1.order_type = t2.order_type) OR (t2.order_type IS NULL))
        ) _
    ) _
    """
    return client.query(query).to_dataframe()
