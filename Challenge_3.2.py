import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pyarrow.parquet as pq

# List of filenames to read
file_names = [
    '20200120.parquet',  # Jan 20, 2020
    '20200820.parquet',  # Aug 20, 2020
    '20210220.parquet',  # Feb 20, 2021
    '20210810.parquet',  # Aug 10, 2021
    '20220120.parquet'   # Jan 20, 2022
]

# Initialize an empty list to store numpy arrays for each dataset
summit_input_powers = []
power_magnitudes = []
power_gradients = []
power_spectrum_magnitudes = []
power_psd = []
positive_frequencies_scaled = []

# Processing steps for each file
for file_name in file_names:
    df = pq.read_table(file_name).to_pandas().drop_duplicates()     # Create dataframe and drop duplicates
    df = df.groupby(['timestamp', 'hostname']).agg({                # Average power values with same hostname and timestamp
        'ps0_input_power': 'mean', 'ps1_input_power': 'mean'
    }).reset_index()
    
    # Calculate input power and create a new column for cabinet names
    df['input_power'] = df['ps0_input_power'] + df['ps1_input_power']
    df['cabinet'] = df['hostname'].str[:3]

    # Group by timestamp and cabinet, then aggregate input_power for the entire system
    df_summit = df.groupby(['timestamp']).agg({'input_power': 'sum'}).reset_index()
    summit_input_powers.append(df_summit['input_power'].values)
    
    # Calculate the power fluctuations
    power_fluctuations = df_summit['input_power'].diff()
    power_fluctuations = power_fluctuations[1:]  # Remove the first row with NaN due to diff() method
    
    # Calculate the power magnitudes
    power_magnitude = np.abs(power_fluctuations)
    power_magnitudes.append(power_magnitude)
    
    # Calculate the power gradients using np.gradient
    power_gradient = np.gradient(df_summit['input_power'].values)
    power_gradients.append(power_gradient)
    
    # Apply FFT to power fluctuations to compute the Power Spectrum
    spectrum = np.fft.fft(power_fluctuations)
    spectrum_real = np.real(spectrum)                   # Take real part of spectrum data
    power_spectrum = np.abs(spectrum_real) ** 2         # Take absolute values and squares them to obtain power spectrum
    power_spectrum_magnitudes.append(power_spectrum)
    
    # Compute the Power Spectral Density (PSD)
    sampling_rate = 1 / 60  # 1 minute = 1/60 hour
    positive_spectrum = spectrum_real[1:len(spectrum) // 2] # Exclude the first element (DC component) and the negative frequencies
    psd = positive_spectrum / (len(power_fluctuations) * sampling_rate)
    power_psd.append(psd)

# Create Figure objects for each chart
fig_power_consumption = make_subplots(rows=1, cols=5, shared_yaxes=True, 
                                     subplot_titles=['Jan 20, 2020', 'Aug 20, 2020', 'Feb 20, 2021', 'Aug 10, 2021', 'Jan 20, 2022'],
                                     horizontal_spacing=0.002)

fig_power_magnitude = make_subplots(rows=1, cols=5, shared_yaxes=True, 
                                    subplot_titles=['Jan 20, 2020', 'Aug 20, 2020', 'Feb 20, 2021', 'Aug 10, 2021', 'Jan 20, 2022'],
                                    horizontal_spacing=0.002)

fig_power_gradient = make_subplots(rows=1, cols=5, shared_yaxes=True, 
                                   subplot_titles=['Jan 20, 2020', 'Aug 20, 2020', 'Feb 20, 2021', 'Aug 10, 2021', 'Jan 20, 2022'],
                                   horizontal_spacing=0.002)

fig_power_spectrum = make_subplots(rows=1, cols=5, shared_yaxes=True, 
                                   subplot_titles=['Jan 20, 2020', 'Aug 20, 2020', 'Feb 20, 2021', 'Aug 10, 2021', 'Jan 20, 2022'],
                                   horizontal_spacing=0.002)

fig_power_psd = make_subplots(rows=1, cols=5, shared_yaxes=True, 
                              subplot_titles=['Jan 20, 2020', 'Aug 20, 2020', 'Feb 20, 2021', 'Aug 10, 2021', 'Jan 20, 2022'],
                              horizontal_spacing=0.002)

# Create a list of colors for each day
colors = ['green', 'blue', 'purple', 'orange', 'red']

# Define tick values and labels for Power Consumption, Power Magnitude, and Power Gradient Charts
tick_values_power_consumption = [360, 720, 1080]
tick_labels_power_consumption = ['6', '12', '18']

# Define tick values and labels for Power Spectrum and Power Spectral Density (PSD) Charts
tick_values_other_charts = [180, 360, 540]
tick_labels_other_charts = ['180', '360', '540']

# Add traces to each subplot with coordinated colors and numpy arrays for Power Consumption, Power Gradient, and Power Spectrum charts
for i, (y_power_consumption, y_power_magnitude, y_power_gradient, y_power_spectrum, y_power_psd) in enumerate(zip(summit_input_powers, power_magnitudes, power_gradients, power_spectrum_magnitudes, power_psd)):
    x_power_consumption = np.arange(len(y_power_consumption))
    x_other_charts = np.fft.fftfreq(len(y_power_magnitude), d=(1 / 60))[1:len(y_power_magnitude) // 2] * 24
    
    # Add traces to each subplot of Power Consumption chart
    fig_power_consumption.add_trace(go.Scatter(x=x_power_consumption, y=y_power_consumption, name=f'Plot {i+1}', line=dict(color=colors[i])),
                                    row=1, col=i+1)
    fig_power_consumption.update_xaxes(tickmode='array', tickvals=tick_values_power_consumption, ticktext=tick_labels_power_consumption, row=1, col=i+1,
                                       tickfont=dict(size=15))
    fig_power_consumption.update_yaxes(tickfont=dict(size=15), row=1, col=i+1)

    # Add traces to each subplot of Power Magnitude chart
    fig_power_magnitude.add_trace(go.Scatter(x=x_power_consumption, y=y_power_magnitude, name=f'Plot {i+1}', line=dict(color=colors[i])),
                                  row=1, col=i+1)
    fig_power_magnitude.update_xaxes(tickmode='array', tickvals=tick_values_power_consumption, ticktext=tick_labels_power_consumption, row=1, col=i+1,
                                     tickfont=dict(size=15))
    fig_power_magnitude.update_yaxes(tickfont=dict(size=15), row=1, col=i+1)
    
    # Add traces to each subplot of Power Gradient chart
    fig_power_gradient.add_trace(go.Scatter(x=x_power_consumption, y=y_power_gradient, name=f'Plot {i+1}', line=dict(color=colors[i])),
                                 row=1, col=i+1)
    fig_power_gradient.update_xaxes(tickmode='array', tickvals=tick_values_power_consumption, ticktext=tick_labels_power_consumption, row=1, col=i+1,
                                    tickfont=dict(size=15))
    fig_power_gradient.update_yaxes(tickfont=dict(size=15), row=1, col=i+1)
    
    # Add traces to each subplot of Power Spectrum chart
    fig_power_spectrum.add_trace(go.Scatter(x=x_other_charts, y=y_power_spectrum, name=f'Plot {i+1}', line=dict(color=colors[i])),
                                 row=1, col=i+1)
    fig_power_spectrum.update_xaxes(tickmode='array', tickvals=tick_values_other_charts, ticktext=tick_labels_other_charts, row=1, col=i+1,
                                    tickfont=dict(size=15))
    fig_power_spectrum.update_yaxes(tickfont=dict(size=15), row=1, col=i+1)

    # Add traces to each subplot of Power Spectral Density chart
    fig_power_psd.add_trace(go.Scatter(x=x_other_charts, y=y_power_psd, name=f'Plot {i+1}', line=dict(color=colors[i])),
                            row=1, col=i+1)
    fig_power_psd.update_xaxes(tickmode='array', tickvals=tick_values_other_charts, ticktext=tick_labels_other_charts, row=1, col=i+1,
                               tickfont=dict(size=15))
    fig_power_psd.update_yaxes(tickfont=dict(size=15), row=1, col=i+1)



# Update layout of the Power Consumption chart
fig_power_consumption.update_layout(height=400, width=1200, title='SUMMIT Power Consumption Comparison',
                                    showlegend=False, margin=dict(l=10, r=10, t=50, b=50),
                                    xaxis=dict(title='<b>Hour of Day<b>', titlefont=dict(size=15)), 
                                    yaxis=dict(title='<b>Power Draw (Watts)<b>', titlefont=dict(size=15)))

# Update layout of the Power Magnitude chart
fig_power_magnitude.update_layout(height=400, width=1200, title='SUMMIT Power Magnitude Comparison',
                                  showlegend=False, margin=dict(l=10, r=10, t=50, b=50),
                                  xaxis=dict(title='<b>Hour of Day<b>', titlefont=dict(size=15)), 
                                  yaxis=dict(title='<b>Power Magnitude (Watts)<b>', titlefont=dict(size=15)))

# Update layout of the Power Gradient chart
fig_power_gradient.update_layout(height=400, width=1200, title='SUMMIT Power Gradient Comparison',
                                 showlegend=False, margin=dict(l=10, r=10, t=50, b=50),
                                 xaxis=dict(title='<b>Hour of Day<b>', titlefont=dict(size=15)), 
                                 yaxis=dict(title='<b>Power Dynamics (W/min)<b>', titlefont=dict(size=15)))

# Update layout of the Power Spectrum chart
fig_power_spectrum.update_layout(height=400, width=1200, title='SUMMIT Power Spectrum Comparison',
                                 showlegend=False, margin=dict(l=10, r=10, t=50, b=50),
                                 xaxis=dict(title='<b>Frequency (720 Cycles per Day)<b>', titlefont=dict(size=15)), 
                                 yaxis=dict(title='<b>Power Spectrum Magnitude (Watts)<b>', titlefont=dict(size=15)))

# Update layout of the Power Spectral Density chart
fig_power_psd.update_layout(height=400, width=1200, title='SUMMIT Power Spectral Density Comparison',
                            showlegend=False, margin=dict(l=10, r=10, t=50, b=50),
                            xaxis=dict(title='<b>Frequency (720 Cycles per Day)<b>', titlefont=dict(size=15)), 
                            yaxis=dict(title='<b>PSD (Watts\u00B2 / Cycles Per Day)<b>', titlefont=dict(size=15)))

# Create shapes to set background color for each subplot
shapes = []
num_plots = 5
num_rows = 1
num_cols = 5
for row in range(1, num_rows + 1):
    for col in range(1, num_cols + 1):
        shape = dict(
            type='rect',
            xref='paper',
            yref='paper',
            x0=(col - 1) / num_cols,
            y0=(row - 1) / num_rows,
            x1=col / num_cols,
            y1=row / num_rows,
            fillcolor=colors[(row - 1) * num_cols + (col - 1)],
            opacity=0.2,
            layer='below',
            line=dict(width=0),
        )
        shapes.append(shape)

# Set shapes in the layout
fig_power_consumption.update_layout(shapes=shapes)
fig_power_magnitude.update_layout(shapes=shapes)
fig_power_gradient.update_layout(shapes=shapes)
fig_power_spectrum.update_layout(shapes=shapes)
fig_power_psd.update_layout(shapes=shapes)

# Write the charts to HTML files
fig_power_consumption.write_html("SUMMIT_Power_Consumption.html")
fig_power_magnitude.write_html("SUMMIT_Power_Magnitude.html")
fig_power_gradient.write_html("SUMMIT_Power_Gradient.html")
fig_power_spectrum.write_html("SUMMIT_Power_Spectrum.html")
fig_power_psd.write_html("SUMMIT_Power_Spectral_Density.html")




