import numpy as np
import pandas as pd
import plotly.express as px
from src.metrics import calculate_metrics


def draw_heatmap(df, metric_list, calc_list, step_y):
    for metric in metric_list:
        for calculation in calc_list:
            heatmap_data = df.pivot(
                index="surge_bin",
                columns="dist_bin",
                values=metric+"."+calculation
            )

            significance_data = df.pivot(
                index="surge_bin",
                columns="dist_bin",
                values=metric+".is_significant"
            )

            text_data = heatmap_data.copy()
            for row in heatmap_data.index:
                for col in heatmap_data.columns:
                    value = heatmap_data.loc[row, col]
                    is_significant = significance_data.loc[row, col]
                    text_data.loc[row, col] = f"{value:.2f}*" if is_significant else f"{value:.2f}"

            fig = px.imshow(
                heatmap_data,
                text_auto=True,
                color_continuous_scale='Viridis',
                aspect='auto'
            )

            fig.update_traces(text=text_data.values, texttemplate="%{text}")
            fig.update_layout(width=700, height=500, yaxis={"tick0": heatmap_data.index,
                                                            "dtick": step_y},
                              title=metric+', '+calculation)

            fig.show()

    return 0


from plotly.subplots import make_subplots
import plotly.graph_objects as go


def draw_heatmap_NEW(df, metric_list, calc_list, step_y):
    for metric in metric_list:
        # Create a subplot figure with one row for each calculation
        fig = make_subplots(
            rows=1,
            cols=len(calc_list),
            subplot_titles=[f"{metric}, {calculation}" for calculation in calc_list]
        )

        for col_idx, calculation in enumerate(calc_list, start=1):
            heatmap_data = df.pivot(
                index="surge_bin",
                columns="dist_bin",
                values=metric + "." + calculation
            )

            significance_data = df.pivot(
                index="surge_bin",
                columns="dist_bin",
                values=metric + ".is_significant"
            )

            text_data = heatmap_data.copy()
            for row in heatmap_data.index:
                for col in heatmap_data.columns:
                    value = heatmap_data.loc[row, col]
                    is_significant = significance_data.loc[row, col]
                    text_data.loc[row, col] = f"{value:.2f}*" if is_significant else f"{value:.2f}"

            # Add a heatmap trace
            heatmap = go.Heatmap(
                z=heatmap_data.values,
                x=heatmap_data.columns,
                y=heatmap_data.index,
                colorscale="Viridis",
                text=text_data.values,
                texttemplate="%{text}",
                showscale=False,
                colorbar_x=0.45 + (col_idx-1)*0.55,
                colorbar=dict(
                    #title=f"{calculation}",
                    # len=0.5,
                    #x=0.1 + 0.5 * (col_idx - 0.3)  # Adjust x position for each colorbar
                )
            )

            fig.add_trace(heatmap, row=1, col=col_idx)

        # Update layout for the entire figure
        fig.update_layout(
            height=500,
            width=700 * len(calc_list),  # Adjust width based on number of calculations
            yaxis=dict(tick0=heatmap_data.index, dtick=step_y),
            title=f"{metric}",
            margin=dict(l=20, r=40, t=50, b=20)  # Adjust margins for better spacing
        )

        fig.show()

        fig.write_html('/Users/georgiinusuev/Desktop/qqq.html')

    return 0


def draw_lines(df_recprice, df_order, df_full, metric_list):
    for i in metric_list:
        ddt = calculate_metrics(
            df_recprice,
            df_order,
            df_full,
            group_cols=['group_name', 'surge_bin', 'orders_distance_bin'],
        ).sort_values(by=['group_name', 'surge_bin', 'orders_distance_bin'])

        ddt = ddt[ddt['group_name'] != 'Before']
        ddt[i[0]] = ddt[i[1]] / ddt[i[2]]
        ddt[['group_name', 'surge_bin', 'orders_distance_bin', i[0]]]

        ddt['group_surge'] = ddt['group_name'] + " | Surge " + ddt['surge_bin'].astype(str)

        fig = px.line(
            ddt,
            x="orders_distance_bin",
            y=i[0],
            color="group_surge",
            markers=True,
            labels={"orders_distance_bin": "Distance Bin, start"},
            title= i[0] + " vs Orders Distance Bin",
        )

        fig.update_layout(
            legend_title="Group | Surge Bin",
            xaxis=dict(title="Orders Distance Bin"),
            yaxis=dict(title=i[0]),
            width=800,
            height=500,
        )

        fig.show()

    return 0
