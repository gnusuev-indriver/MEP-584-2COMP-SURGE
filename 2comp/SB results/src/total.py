import warnings
warnings.filterwarnings("ignore")

import pathlib
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import timedelta

from src.download import download_experiment_data, download_recprice_data, download_order_data
from src.prepare import prepare_recprice_data, prepare_order_data, get_full_df
from src.metrics import calculate_metrics, get_switchback_results, get_metrics
from src.draw import draw_heatmap, draw_lines

# Parameters
USER_NAME = 'nusuev'
EXP_ID = 2102
DAYS_BEFORE = 0
DATA_ROOT_PATH = pathlib.Path(f'data/exp_id={EXP_ID}')
if not DATA_ROOT_PATH.exists():
    DATA_ROOT_PATH.mkdir(parents=True, exist_ok=True)

# Data
## Experiment Data
df_exp = download_experiment_data(exp_id=EXP_ID, user_name=USER_NAME)
df_exp['hour'] = df_exp['switch_start_dttm'].dt.hour
df_exp['hour'] = df_exp['hour'].astype('category')

EXP_START_DATE = df_exp.utc_start_dttm.dt.date.astype('str').iloc[0]
EXP_STOP_DATE = df_exp.utc_finish_dttm.dt.date.astype('str').iloc[0]
BEFORE_START_DATE = (df_exp.utc_start_dttm.dt.date - timedelta(days=DAYS_BEFORE)).astype('str').iloc[0]
CITY_ID = df_exp.city_id.iloc[0]
ORDER_TYPE = df_exp.order_type.iloc[0]
EXP_NAME = df_exp.exp_name.iloc[0]

print(
    f"""
    exp_start_date: {EXP_START_DATE}
    exp_stop_date: {EXP_STOP_DATE}
    before_start_date: {BEFORE_START_DATE}
    city_id: {CITY_ID}
    order_type: {ORDER_TYPE}
    exp_name: {EXP_NAME}
    """
)

## Recprice Data
df_recprice = download_recprice_data(
    start_date=BEFORE_START_DATE,
    stop_date=EXP_STOP_DATE,
    city_id=CITY_ID,
    order_type=ORDER_TYPE,
    user_name=USER_NAME,
)

## Order Data
df_orders = download_order_data(
    start_date=BEFORE_START_DATE,
    stop_date=EXP_STOP_DATE,
    city_id=CITY_ID,
    order_type=ORDER_TYPE,
    user_name=USER_NAME,
)

# Prepare Data
df_recprice_prepared = prepare_recprice_data(df_recprice)
df_orders_prepared = prepare_order_data(df_orders)
df_full = get_full_df(df_recprice_prepared, df_orders_prepared)
df_full['group_name'] = df_full['recprice_group_name']

bound_dynamic_surge = 1.0
step_surge_bin = 0.5
step_orders_distance_bin = 1

filtered_surge_bin = np.unique([1.0, 1.5, 2.0])
filtered_dist_bins = np.arange(0, 25 + 1, step_orders_distance_bin)

# df_full
df_full = df_full[df_full['original_dynamic_surge_updated'] > bound_dynamic_surge]
df_full['surge_bin'] = (df_full['original_dynamic_surge_updated'] // step_surge_bin) * step_surge_bin
df_full['orders_distance_bin'] = (df_full['distance_in_km'] // step_orders_distance_bin) * step_orders_distance_bin
df_full['surge_bin'] = df_full['surge_bin'].clip(upper=max(filtered_surge_bin))
df_full['orders_distance_bin'] = df_full['orders_distance_bin'].clip(upper=max(filtered_dist_bins))

# df_recprice
df_recprice_prepared_merged = df_recprice_prepared.merge(df_orders_prepared[['calcprice_uuid', 'distance_in_km']],
                                                         on=['calcprice_uuid'], how='left')
df_recprice_prepared_merged = df_recprice_prepared_merged[
    df_recprice_prepared_merged['original_dynamic_surge_updated'] > bound_dynamic_surge]
df_recprice_prepared_merged['surge_bin'] = (df_recprice_prepared_merged[
                                                'original_dynamic_surge_updated'] // step_surge_bin) * step_surge_bin
df_recprice_prepared_merged['orders_distance_bin'] = (df_recprice_prepared_merged[
                                                          'distance_in_km'] // step_orders_distance_bin) * step_orders_distance_bin
df_recprice_prepared_merged['surge_bin'] = df_recprice_prepared_merged['surge_bin'].clip(upper=max(filtered_surge_bin))
df_recprice_prepared_merged['orders_distance_bin'] = df_recprice_prepared_merged['orders_distance_bin'].clip(
    upper=max(filtered_dist_bins))

# df_orders
df_orders_prepared_merged = df_orders_prepared.merge(
    df_recprice_prepared[['calcprice_uuid', 'original_dynamic_surge_updated']],
    on=['calcprice_uuid'], how='left')
df_orders_prepared_merged = df_orders_prepared_merged[
    df_orders_prepared_merged['original_dynamic_surge_updated'] > bound_dynamic_surge]
df_orders_prepared_merged['surge_bin'] = (df_orders_prepared_merged[
                                              'original_dynamic_surge_updated'] // step_surge_bin) * step_surge_bin
df_orders_prepared_merged['orders_distance_bin'] = (df_orders_prepared_merged[
                                                        'distance_in_km'] // step_orders_distance_bin) * step_orders_distance_bin
df_orders_prepared_merged['surge_bin'] = df_orders_prepared_merged['surge_bin'].clip(upper=max(filtered_surge_bin))
df_orders_prepared_merged['orders_distance_bin'] = df_orders_prepared_merged['orders_distance_bin'].clip(
    upper=max(filtered_dist_bins))

### Metrics
df_metrics_total = calculate_metrics(
    df_recprice_prepared,
    df_orders_prepared,
    df_full,
    group_cols=['group_name', 'switch_start_dttm', 'switch_finish_dttm'],
)

metrics_total_tbl = get_switchback_results(df_metrics_total, alpha=0.05)[
    ['metric', 'control_value', 'experimental_value', 'uplift_rel', 'pvalue', 'is_significant']
]

results = []
for surge_bin in filtered_surge_bin:
    for dist_bin in filtered_dist_bins:
        df_metrics_total = calculate_metrics(
            df_recprice_prepared_merged[(df_recprice_prepared_merged['surge_bin'] == surge_bin) &
                                        (df_recprice_prepared_merged['orders_distance_bin'] == dist_bin)],
            df_orders_prepared_merged[(df_orders_prepared_merged['surge_bin'] == surge_bin) &
                                      (df_orders_prepared_merged['orders_distance_bin'] == dist_bin)],
            df_full[(df_full['surge_bin'] == surge_bin) &
                    (df_full['orders_distance_bin'] == dist_bin)],
            group_cols=['group_name', 'switch_start_dttm', 'switch_finish_dttm'],
        )

        print(surge_bin, dist_bin)
        metrics_total_tbl = get_switchback_results(df_metrics_total, alpha=0.05)

        single_row = {}
        for _, row in metrics_total_tbl.iterrows():
            metric_name = row['metric']
            for stat_name in metrics_total_tbl.columns:
                single_row[f"{metric_name}.{stat_name}"] = row[stat_name]
        df_single_row = pd.DataFrame([single_row])

        metrics_row = {'surge_bin': surge_bin, 'orders_distance_bin': dist_bin}
        metrics_row.update(df_single_row.iloc[0].to_dict())  # Assuming one row in df_metrics_total

        results.append(metrics_row)

df_results = pd.DataFrame(results)
df_results

draw_heatmap(df_results, ['balance', 'cp2order'], ['uplift_abs', 'uplift_rel'], 0.5)
draw_lines(df_recprice_prepared_merged,
           df_orders_prepared_merged,
           df_full,
           [["rides_price_highrate_usd", "rides_price_highrate_usd_sum", "rides_count"],
            ["price_highrate_usd", "price_highrate_usd_sum", "orders_count"],
            ["price_done_usd", "price_done_usd_sum", "rides_count"],
            ["done2rec", "price_done_usd_sum", "rides_price_highrate_usd_sum"],
            ["good_rate", "good_orders_count", "orders_count"],
            ["balance", "good_orders_count", "rides_count"],
            ["original_dynamic_surge_updated", "original_dynamic_surge_updated_sum", "calcprices_count"],
            ["cp2order", "orders_count", "calcprices_count"],
            ["cp2done", "rides_count", "calcprices_count"]])
