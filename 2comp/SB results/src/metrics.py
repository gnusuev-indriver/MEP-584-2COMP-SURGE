import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

from .pipeline import RatioMetricHypothesisTestingPipeline


METRIC_LIST = [
    # conversions
    ### calcprice --> order
    ["cp2order", "orders_count", "calcprices_count"],
    ### order --> ...
    ["order2bid", "orders_with_bids_count", "orders_count"],
    ["order2start_price_bid", "start_price_bid_orders_count", "orders_count"],
    ["order2accept", "accepted_orders_count", "orders_count"],
    ["order2done", "rides_count", "orders_count"],
    ### bid --> ...
    ["bid2accept", "accepted_orders_count", "orders_with_bids_count"],
    ["bid2done", "rides_count", "orders_with_bids_count"],
    ["start_price_bid2accept", "start_price_bid_accepted_orders_count", "start_price_bid_orders_count"],
    ["start_price_bid2done", "start_price_bid_rides_count", "start_price_bid_orders_count"],
    ### calcprice --> ...
    ["cp2bid", "start_price_bid_orders_count", "calcprices_count"],
    ["cp2start_price_bid", "start_price_bid_orders_count", "calcprices_count"],
    ["cp2accept", "accepted_orders_count", "calcprices_count"],
    ["cp2done", "rides_count", "calcprices_count"],
    # prices
    ["minprice_usd", "minprice_usd_sum", "calcprices_count"],
    ["price_base_usd", "price_base_usd_sum", "calcprices_count"],
    ["recprice_usd", "recprice_usd_sum", "calcprices_count"],
    ["price_highrate_usd", "price_highrate_usd_sum", "orders_count"],
    ["rides_price_highrate_usd", "rides_price_highrate_usd_sum", "rides_count"],
    ["price_start_usd", "price_start_usd_sum", "orders_count"],
    ["rides_price_start_usd", "rides_price_start_usd_sum", "rides_count"],
    ["price_tender_usd", "price_tender_usd_sum", "tenders_count"],
    ["price_done_usd", "price_done_usd_sum", "rides_count"],
    ########
    ["done2rec", "price_done_usd_sum", "rides_price_highrate_usd_sum"],
    # surge
    ["surge", "surge_sum", "calcprices_count"],
    ["dynamic_surge", "dynamic_surge_sum", "calcprices_count"],
    ["original_dynamic_surge_updated", "original_dynamic_surge_updated_sum", "calcprices_count"],
    # ["surge_gr_1", "surge_gr_1_sum", "surge_gr_1_calcprices_count"],
    # ["surge_le_1", "surge_le_1_sum", "surge_le_1_calcprices_count"],
    # ["surge_gr_1_coverage", "surge_gr_1_calcprices_count", "calcprices_count"],
    # ["surge_le_1_coverage", "surge_le_1_calcprices_count", "calcprices_count"],
    # balance
    ["good_rate", "good_orders_count", "orders_count"],
    ["balance", "good_orders_count", "rides_count"],
    # by segment
    ### segment: minprice
    ["orders_by_minprice_share", "orders_by_minprice_count", "orders_count"],
    ["cp2order_by_orders_by_minprice", "orders_by_minprice_count", "calcprices_count"],
    ["order2done_by_orders_by_minprice", "rides_by_minprice_count", "orders_by_minprice_count"],
    ["cp2done_by_orders_by_minprice", "rides_by_minprice_count", "calcprices_count"],
    ### segment: surge
    ["surge_gr_1_orders_share", "surge_gr_1_orders_count", "orders_count"],
    ["surge_le_1_orders_share", "surge_le_1_orders_count", "orders_count"],
    ["surge_gr_1_rides_share", "surge_gr_1_rides_count", "rides_count"],
    ["surge_le_1_rides_share", "surge_le_1_rides_count", "rides_count"],
    ["surge_gr_1_cp2order", "surge_gr_1_orders_count", "surge_gr_1_calcprices_count"],
    ["surge_le_1_cp2order", "surge_le_1_orders_count", "surge_le_1_calcprices_count"],
    ["surge_gr_1_order2done", "surge_gr_1_rides_count", "surge_gr_1_orders_count"],
    ["surge_le_1_order2done", "surge_le_1_rides_count", "surge_le_1_orders_count"],
    ["surge_gr_1_cp2done", "surge_gr_1_rides_count", "surge_gr_1_calcprices_count"],
    ["surge_le_1_cp2done", "surge_le_1_rides_count", "surge_le_1_calcprices_count"]
]


def get_switchback_results(df, alpha, metric_list=METRIC_LIST, groups={"control":"Control", "treatment":"A"}):
    res_list = []
    for i in metric_list:
        pipeline = RatioMetricHypothesisTestingPipeline(df, i[0], i[1], i[2], groups)
        try: pipeline.run()
        except ValueError as VE: print('ValueError: {VE}', i[1], i[2])
        except KeyError as KE: print('KeyError: {KE}', i[1], i[2])
        res_list.append(pipeline.result)
    df_res = pd.DataFrame(res_list)
    df_res[f'is_significant'] = df_res['pvalue'] < alpha
    return df_res


# from `indriver-e6e40.emart.incity_detail`
def metric_orders_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(orders_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_tenders_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(tenders_count=('tenders_count', 'sum'))
            .reset_index())

def metric_orders_with_bids_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(orders_with_bids_count=('is_order_with_tender', 'sum'))
            .reset_index())

def metric_start_price_bid_orders_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(start_price_bid_orders_count=('is_order_start_price_bid', 'sum'))
            .reset_index())

def metric_start_price_bid_accepted_orders_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(start_price_bid_accepted_orders_count=('is_order_accepted_start_price_bid', 'sum'))
            .reset_index())

def metric_start_price_bid_rides_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(start_price_bid_rides_count=('is_order_done_start_price_bid', 'sum'))
            .reset_index())

def metric_accepted_orders_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(accepted_orders_count=('is_order_accepted', 'sum'))
            .reset_index())

def metric_rides_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(rides_count=('is_order_done', 'sum'))
            .reset_index())

def metric_price_start_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(price_start_usd_sum=('price_start_usd', 'sum'))
            .reset_index())

def metric_rides_price_start_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(rides_price_start_usd_sum=('rides_price_start_usd', 'sum'))
            .reset_index())

def metric_price_highrate_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(price_highrate_usd_sum=('price_highrate_usd', 'sum'))
            .reset_index())

def metric_rides_price_highrate_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(rides_price_highrate_usd_sum=('rides_price_highrate_usd', 'sum'))
            .reset_index())

def metric_price_tender_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(price_tender_usd_sum=('price_tender_usd', 'sum'))
            .reset_index())

def metric_price_done_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(price_done_usd_sum=('price_done_usd', 'sum'))
            .reset_index())

def metric_good_orders_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(good_orders_count=('is_order_good', 'sum'))
            .reset_index())


# from `indriver-e6e40.ods_recprice_cdc.pricing_logs`
def metric_calcprices_count(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(calcprices_count=('calcprice_uuid', 'nunique'))
            .reset_index())

def metric_price_base_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(price_base_usd_sum=('price_base_usd', 'sum'))
            .reset_index())

def metric_recprice_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(recprice_usd_sum=('recprice_usd', 'sum'))
            .reset_index())

def metric_minprice_usd_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(minprice_usd_sum=('minprice_usd', 'sum'))
            .reset_index())

def metric_surge_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(surge_sum=('surge', 'sum'))
            .reset_index())

def metric_dynamic_surge_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(dynamic_surge_sum=('dynamic_surge', 'sum'))
            .reset_index())

def metric_original_dynamic_surge_updated_sum(df, group_cols):
    return (df
            .groupby(group_cols)
            .agg(original_dynamic_surge_updated_sum=('original_dynamic_surge_updated', 'sum'))
            .reset_index())

def metric_surge_gr_1_sum(df, group_cols):
    return (df[df.surge > 1]
            .groupby(group_cols)
            .agg(surge_gr_1_sum=('surge', 'sum'))
            .reset_index())

def metric_surge_gr_1_calcprices_count(df, group_cols):
    return (df[df.surge > 1]
            .groupby(group_cols)
            .agg(surge_gr_1_calcprices_count=('calcprice_uuid', 'nunique'))
            .reset_index())

def metric_surge_le_1_sum(df, group_cols):
    return (df[df.surge <= 1]
            .groupby(group_cols)
            .agg(surge_le_1_sum=('surge', 'sum'))
            .reset_index())

def metric_surge_le_1_calcprices_count(df, group_cols):
    return (df[df.surge <= 1]
            .groupby(group_cols)
            .agg(surge_le_1_calcprices_count=('calcprice_uuid', 'nunique'))
            .reset_index())


# from incity_and_pricing_tbl
def metric_orders_by_minprice_count(df, group_cols):
    return (df[(df.price_start_usd >= df.minprice_usd*0.99) & (df.price_start_usd <= df.minprice_usd*1.01)]
            .groupby(group_cols)
            .agg(orders_by_minprice_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_orders_by_minprice_with_bids_count(df, group_cols):
    return (df[(df.price_start_usd >= df.minprice_usd*0.99) & (df.price_start_usd <= df.minprice_usd*1.01) & (df.is_order_with_tender)]
            .groupby(group_cols)
            .agg(orders_by_minprice_with_bids_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_accepted_orders_by_minprice_count(df, group_cols):
    return (df[(df.price_start_usd >= df.minprice_usd*0.99) & (df.price_start_usd <= df.minprice_usd*1.01) & (df.is_order_accepted)]
            .groupby(group_cols)
            .agg(accepted_orders_by_minprice_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_rides_by_minprice_count(df, group_cols):
    return (df[(df.price_start_usd >= df.minprice_usd*0.99) & (df.price_start_usd <= df.minprice_usd*1.01) & (df.is_order_done)]
            .groupby(group_cols)
            .agg(rides_by_minprice_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_gr_1_orders_count(df, group_cols):
    return (df[df.surge > 1]
            .groupby(group_cols)
            .agg(surge_gr_1_orders_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_le_1_orders_count(df, group_cols):
    return (df[df.surge <= 1]
            .groupby(group_cols)
            .agg(surge_le_1_orders_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_gr_1_orders_with_bids_count(df, group_cols):
    return (df[(df.surge > 1) & (df.is_order_with_tender)]
            .groupby(group_cols)
            .agg(surge_gr_1_orders_with_bids_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_le_1_orders_with_bids_count(df, group_cols):
    return (df[(df.surge <= 1) & (df.is_order_with_tender)]
            .groupby(group_cols)
            .agg(surge_le_1_orders_with_bids_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_gr_1_start_price_bid_orders_count(df, group_cols):
    return (df[(df.surge > 1) & (df.is_order_start_price_bid)]
            .groupby(group_cols)
            .agg(surge_gr_1_start_price_bid_orders_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_le_1_start_price_bid_orders_count(df, group_cols):
    return (df[(df.surge <= 1) & (df.is_order_start_price_bid)]
            .groupby(group_cols)
            .agg(surge_le_1_start_price_bid_orders_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_gr_1_accepted_orders_count(df, group_cols):
    return (df[(df.surge > 1) & (df.is_order_accepted)]
            .groupby(group_cols)
            .agg(surge_gr_1_accepted_orders_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_le_1_accepted_orders_count(df, group_cols):
    return (df[(df.surge <= 1) & (df.is_order_accepted)]
            .groupby(group_cols)
            .agg(surge_le_1_accepted_orders_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_gr_1_rides_count(df, group_cols):
    return (df[(df.surge > 1) & (df.is_order_done)]
            .groupby(group_cols)
            .agg(surge_gr_1_rides_count=('order_uuid', 'nunique'))
            .reset_index())

def metric_surge_le_1_rides_count(df, group_cols):
    return (df[(df.surge <= 1) & (df.is_order_done)]
            .groupby(group_cols)
            .agg(surge_le_1_rides_count=('order_uuid', 'nunique'))
            .reset_index())


def calculate_metrics(df_recprice, df_order, df_full, group_cols):
    dfm = (metric_calcprices_count(df_recprice, group_cols)
           .merge(metric_orders_count(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_tenders_count(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_orders_with_bids_count(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_start_price_bid_orders_count(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_start_price_bid_accepted_orders_count(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_start_price_bid_rides_count(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_orders_count(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_rides_count(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_price_start_usd_sum(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_rides_price_start_usd_sum(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_price_highrate_usd_sum(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_rides_price_highrate_usd_sum(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_price_tender_usd_sum(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_price_done_usd_sum(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_good_orders_count(df_order, group_cols), on=group_cols, how='left')
           .merge(metric_price_base_usd_sum(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_recprice_usd_sum(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_minprice_usd_sum(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_sum(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_dynamic_surge_sum(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_original_dynamic_surge_updated_sum(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_gr_1_sum(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_gr_1_calcprices_count(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_le_1_sum(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_surge_le_1_calcprices_count(df_recprice, group_cols), on=group_cols, how='left')
           .merge(metric_orders_by_minprice_count(df_full, group_cols), on=group_cols, how='left')
           .merge(metric_orders_by_minprice_with_bids_count(df_full, group_cols), on=group_cols, how='left')
           .merge(metric_accepted_orders_by_minprice_count(df_full, group_cols), on=group_cols, how='left')
           .merge(metric_rides_by_minprice_count(df_full, group_cols), on=group_cols, how='left')
           .merge(metric_surge_gr_1_orders_count(df_full, group_cols), on=group_cols, how='left')
           .merge(metric_surge_le_1_orders_count(df_full, group_cols), on=group_cols, how='left')
           .merge(metric_surge_gr_1_orders_with_bids_count(df_full, group_cols), on=group_cols, how='left')
           .merge(metric_surge_le_1_orders_with_bids_count(df_full, group_cols), on=group_cols, how='left')
           .merge(metric_surge_gr_1_start_price_bid_orders_count(df_full, group_cols), on=group_cols, how='left')
           .merge(metric_surge_le_1_start_price_bid_orders_count(df_full, group_cols), on=group_cols, how='left')
           .merge(metric_surge_gr_1_accepted_orders_count(df_full, group_cols), on=group_cols, how='left')
           .merge(metric_surge_le_1_accepted_orders_count(df_full, group_cols), on=group_cols, how='left')
           .merge(metric_surge_gr_1_rides_count(df_full, group_cols), on=group_cols, how='left')
           .merge(metric_surge_le_1_rides_count(df_full, group_cols), on=group_cols, how='left'))
    return dfm

           
def get_metrics(df, metric_list=METRIC_LIST):
    for metric, num, den in metric_list:
        try:
            df[metric] = df[num] / df[den]
        except:
            df[metric] = np.nan
    return df