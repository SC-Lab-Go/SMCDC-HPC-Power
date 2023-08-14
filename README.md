# SMC23_DataChallenge
This data challenge involves analyzing telemetry data from the SUMMIT supercomputer to gain insights into power consumption patterns and system dynamics. The Python scripts provided in this repository process the large-scale Parquet files and generate interactive visualizations for comparative analysis, enabling a deeper understanding of the system's behavior on specific dates.

## Data
The telemetry data is stored in Parquet files. Unfortunately, due to their large size, the Parquet files are not included in this GitHub repository. However, you can obtain the necessary Parquet files from the SUMMIT supercomputer data repository. By modifying the `file_names` list in the scripts, you can analyze other Parquet files associated with the SUMMIT telemetry.

## Requirements
Besides the neccesary Parquet files, you will need the following Python libraries:
- pyarrow
- numpy 
- pandas
- plotly

Prepare your telemetry data in Parquet format. Please ensure that the Parquet files are compatible with the expected data structure for the programs to work correctly.

Place your Parquet files in the same directory as the Python programs. If your files are too large to be uploaded to GitHub, make sure they are available locally.

## Program 1: Challenge_3.1.py
The script `Challenge_3.1.py` processes the telemetry data and generates interactive HTML visualizations. It performs the following steps for each file in the `file_names` list:
- Creates DataFrames from the Parquet files and removes duplicate rows.
- Averages values with the same hostname and timestamp.
- Calculates the average temperature of each node and the input power for each timestamp.
- Creates Node Scatter, Node Time Series, and Cabinet Time Series plots for each date.
- Exports the plots as interactive HTML files.


## Program 2: Challenge_3.2.py
The script `Challenge_3.2.py` focuses on comparative analysis of power data from five Parquet files related to the SUMMIT supercomputer. It performs the following steps for each file in the `file_names` list:
- Creates DataFrames from the Parquet files and drops duplicate rows.
- Averages power values with the same hostname and timestamp.
- Calculates the input power and creates a new column for cabinet names.
- Groups the data by timestamp and cabinet, then aggregates input power for the entire system.
- Calculates power fluctuations, power magnitudes, and power gradients using numpy and pandas.
- Applies Fast Fourier Transform (FFT) to power fluctuations to compute the Power Spectrum and Power Spectral Density (PSD).
- Generates five sets of visualizations, each consisting of five subplots for Power Consumption, Power Magnitude, Power Gradient, Power Spectrum, and PSD charts, respectively.
- Customizes the visualizations with coordinated colors and tick values for easy comparison.
- Writes the visualizations to separate HTML files for interactive exploration.

The comparative analysis enabled by Challenge_3.2 allows users to gain valuable insights into the power dynamics and fluctuations of the SUMMIT system on five different dates, making it easier to identify patterns and trends across multiple datasets. 

## Instructions
To use the scripts, follow these steps:
1. Obtain the required Parquet files from the SUMMIT supercomputer data repository.
2. Save the Parquet files in the same directory as the Python scripts.
3. Adjust the `file_names` list in the scripts to include the filenames of the Parquet files you want to analyze.
4. Run the scripts using Python, and the visualizations will be generated as interactive HTML files.
5. Open the HTML files in your web browser to explore and analyze the telemetry data.

## Note
Please be aware that processing an incorrect number of Parquet files simultaneously with Program 2 may result in unexpected results. It is recommended to use Program 2 for comparative analysis with of five Parquet files at a time to ensure clarity and ease of interpretation. If it is desired to process more or less Parquet files than five, the code relating to the plotly chart generation will need to be augmented. 

For any inquiries or issues related to this project, feel free to contact the repository owner or author.

---
Author: Joseph Caldwell  
Date: 8/1/23
