import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import plotly.express as px

# List of filenames to read
file_names = [
    '20200120.parquet',  # Jan 20, 2020
    '20200820.parquet',  # Aug 20, 2020
    '20210220.parquet',  # Feb 20, 2021
    '20210810.parquet',  # Aug 10, 2021
    '20220120.parquet'   # Jan 20, 2022
]

# Initialize an empty list to store DataFrames
dataframes = []

# Datasets contain issues where conversion between 1hz resolution to 1min resolution resulted 
# in multiple instances of rows that contain same timestamp and hostname.
# Below code rectifies this issue by dropping duplicate rows, then
# taking averages of all associated values of rows matching same hostname and timestamp

# Processing steps for each file
for file_name in file_names:
    df = pq.read_table(file_name).to_pandas().drop_duplicates()     #create dataframe and drop duplicates
    df = df.groupby(['timestamp', 'hostname']).agg({                #average values with same hostname and timestamp
        'p0_gpu0_power': 'mean', 'p0_gpu1_power': 'mean', 'p0_gpu2_power': 'mean',
        'p1_gpu0_power': 'mean', 'p1_gpu1_power': 'mean', 'p1_gpu2_power': 'mean',
        'p0_power': 'mean', 'p1_power': 'mean', 'gpu0_core_temp': 'mean',
        'gpu0_mem_temp': 'mean', 'gpu1_core_temp': 'mean', 'gpu1_mem_temp': 'mean',
        'gpu2_core_temp': 'mean', 'gpu2_mem_temp': 'mean', 'gpu3_core_temp': 'mean',
        'gpu3_mem_temp': 'mean', 'gpu4_core_temp': 'mean', 'gpu4_mem_temp': 'mean',
        'gpu5_core_temp': 'mean', 'gpu5_mem_temp': 'mean', 'p0_core0_temp': 'mean',
        'p0_core1_temp': 'mean', 'p0_core2_temp': 'mean', 'p0_core3_temp': 'mean',
        'p0_core4_temp': 'mean', 'p0_core5_temp': 'mean', 'p0_core6_temp': 'mean',
        'p0_core7_temp': 'mean', 'p0_core8_temp': 'mean', 'p0_core9_temp': 'mean',
        'p0_core10_temp': 'mean', 'p0_core11_temp': 'mean', 'p0_core12_temp': 'mean',
        'p0_core14_temp': 'mean', 'p0_core15_temp': 'mean', 'p0_core16_temp': 'mean',
        'p0_core17_temp': 'mean', 'p0_core18_temp': 'mean', 'p0_core19_temp': 'mean',
        'p0_core20_temp': 'mean', 'p0_core21_temp': 'mean', 'p0_core22_temp': 'mean',
        'p0_core23_temp': 'mean', 'p1_core0_temp': 'mean', 'p1_core1_temp': 'mean',
        'p1_core2_temp': 'mean', 'p1_core3_temp': 'mean', 'p1_core4_temp': 'mean',
        'p1_core5_temp': 'mean', 'p1_core6_temp': 'mean', 'p1_core7_temp': 'mean',
        'p1_core8_temp': 'mean', 'p1_core9_temp': 'mean', 'p1_core10_temp': 'mean',
        'p1_core11_temp': 'mean', 'p1_core12_temp': 'mean', 'p1_core14_temp': 'mean',
        'p1_core15_temp': 'mean', 'p1_core16_temp': 'mean', 'p1_core17_temp': 'mean',
        'p1_core18_temp': 'mean', 'p1_core19_temp': 'mean', 'p1_core20_temp': 'mean',
        'p1_core21_temp': 'mean', 'p1_core22_temp': 'mean', 'p1_core23_temp': 'mean',
        'ps0_input_power': 'mean', 'ps1_input_power': 'mean'
    }).reset_index()

    # calculates average temperature of node
    df['node_temp_mean'] = df[['gpu0_core_temp', 'gpu1_core_temp', 'gpu2_core_temp', 'gpu3_core_temp', 'gpu4_core_temp',
                               'gpu5_core_temp', 'gpu0_mem_temp', 'gpu1_mem_temp', 'gpu2_mem_temp', 'gpu3_mem_temp',
                               'gpu4_mem_temp', 'gpu5_mem_temp', 'p0_core0_temp', 'p0_core1_temp', 'p0_core2_temp',
                               'p0_core3_temp', 'p0_core4_temp', 'p0_core5_temp', 'p0_core6_temp', 'p0_core7_temp',
                               'p0_core8_temp', 'p0_core9_temp', 'p0_core10_temp', 'p0_core11_temp', 'p0_core12_temp',
                               'p0_core14_temp', 'p0_core15_temp', 'p0_core16_temp', 'p0_core17_temp', 'p0_core18_temp',
                               'p0_core19_temp', 'p0_core20_temp', 'p0_core21_temp', 'p0_core22_temp', 'p0_core23_temp',
                               'p1_core0_temp', 'p1_core1_temp', 'p1_core2_temp', 'p1_core3_temp', 'p1_core4_temp',
                               'p1_core5_temp', 'p1_core6_temp', 'p1_core7_temp', 'p1_core8_temp', 'p1_core9_temp',
                               'p1_core10_temp', 'p1_core11_temp', 'p1_core12_temp', 'p1_core14_temp', 'p1_core15_temp',
                               'p1_core16_temp', 'p1_core17_temp', 'p1_core18_temp', 'p1_core19_temp', 'p1_core20_temp',
                               'p1_core21_temp', 'p1_core22_temp', 'p1_core23_temp']].mean(axis=1)
    # calculates input power
    df['input_power'] = df.apply(lambda row: row.ps0_input_power + row.ps1_input_power, axis=1)
    df['cabinet'] = df['hostname'].str[:3]  #makes new column for cabinet names for future processing

    # Append the processed DataFrame to the list
    dataframes.append(df)

# Initialize an empty list to store the new DataFrames
new_dataframes = []

# Loop through each DataFrame in the dataframes list
for df in dataframes:
    # Create a new DataFrame with the specified columns
    new_df = df[['timestamp', 'cabinet', 'hostname', 'input_power', 'node_temp_mean']].copy()
    new_dataframes.append(new_df)

# Dates corresponding to each DataFrame
dates = ['Jan 20, 2020', 'Aug 20, 2020', 'Feb 20, 2021', 'Aug 10, 2021', 'Jan 20, 2022']

# Loop through each DataFrame and create the "NodeScatter," "NodeTimeSeries," and "CabinetTimeSeries" plots
for i, df in enumerate(new_dataframes):
    # Get the corresponding timestamp
    date = dates[i]

    # Create the Node Scatter Plot
    scatter_fig = px.scatter(df, x="input_power", y="node_temp_mean", color="cabinet",
                            hover_name="hostname", hover_data=["timestamp"],
                            labels={
                                "input_power": "Input Power",
                                "node_temp_mean": "Node Average Temp",
                                "cabinet": "Cabinet",
                                "timestamp": "Timestamp",
                            }, 
                            title=f"SUMMIT: Node Telemetry ({date})")
    scatter_fig.update_xaxes(tickfont=dict(size=15))
    scatter_fig.update_yaxes(tickfont=dict(size=15))
    scatter_fig.update_layout(title=None)
    scatter_fig.update_layout(margin=dict(l=10, r=10, t=50, b=50),
                              xaxis=dict(title='<b>Input Power (Watts)<b>', titlefont=dict(size=15)),
                              yaxis=dict(title='<b>Node Average Temperature (Celsius)<b>', titlefont=dict(size=15)))
    scatter_output_filename = f"{date.replace(' ', '').replace(',', '').replace(':', '')}_NodeScatterPlot.html"
    scatter_fig.write_html(scatter_output_filename)    # Create html for interactive plot

    # Create the Node Time Series Plot
    time_series_fig = px.scatter(df, x="input_power", y="node_temp_mean", animation_frame="timestamp",
                                 animation_group="hostname", color="cabinet", hover_name="hostname",
                                 range_x=[0, 2800], range_y=[0, 50],    # May need to adjust ranges depending on data
                                 labels={
                                     "input_power": "Input Power",
                                     "node_temp_mean": "Node Average Temp",
                                     "cabinet": "Cabinet",
                                     "timestamp": "Timestamp"
                                 },
                                 title=f"SUMMIT: Node Telemetry Time Series ({date})")
    time_series_fig.update_xaxes(tickfont=dict(size=15))
    time_series_fig.update_yaxes(tickfont=dict(size=15))
    time_series_fig.update_layout(margin=dict(l=10, r=10, t=50, b=50),
                                  xaxis=dict(title='<b>Input Power (Watts)<b>', titlefont=dict(size=15)),
                                  yaxis=dict(title='<b>Node Average Temperature (Celsius)<b>', titlefont=dict(size=15)))
    time_series_fig.update_layout(title=None)
    time_series_output_filename = f"{date.replace(' ', '').replace(',', '').replace(':', '')}_NodeTimeSeries.html"
    time_series_fig.write_html(time_series_output_filename)    # Create html for interactive plot

    # Extra processing step for the Cabinet Time Series Plot, adds input power and averages temps across all nodes in each cabinet
    df_cabinets = df.groupby(['timestamp', 'cabinet']).agg({'input_power': 'sum', 'node_temp_mean': 'mean'}).reset_index()

    # Create the Cabinet Time Series plot
    cabinet_time_series_fig = px.scatter(df_cabinets, x="input_power", y="node_temp_mean", animation_frame="timestamp",
                                         animation_group="cabinet", color="cabinet", hover_name="cabinet",
                                         range_x=[0, 45000], range_y=[0, 50],   # May need to adjust ranges depending on data
                                         labels={
                                             "input_power": "Input Power",
                                             "node_temp_mean": "Cabinet Average Temp",
                                             "cabinet": "Cabinet",
                                             "timestamp": "Timestamp"
                                         },
                                         title=f"SUMMIT: Cabinet Telemetry Time Series ({date})")
    cabinet_time_series_fig.update_xaxes(tickfont=dict(size=15))
    cabinet_time_series_fig.update_yaxes(tickfont=dict(size=15))
    cabinet_time_series_fig.update_layout(margin=dict(l=10, r=10, t=50, b=50),
                                          xaxis=dict(title='<b>Input Power (Watts)<b>', titlefont=dict(size=15)),
                                          yaxis=dict(title='<b>Cabinet Average Temperature (Celsius)<b>', titlefont=dict(size=15)))
    cabinet_time_series_fig.update_layout(title=None)
    cabinet_time_series_output_filename = f"{date.replace(' ', '').replace(',', '').replace(':', '')}_CabinetTimeSeries.html"
    cabinet_time_series_fig.write_html(cabinet_time_series_output_filename)    # Create html for interactive plot
