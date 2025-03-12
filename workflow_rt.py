import os
import subprocess
import json
import re
import sys
import argparse
import pandas as pd
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go
from datetime import datetime

def extract_workflow_ids(data):
    '''
    Extracts workflow ids from a JSON file and returns it as a list. 
    Note: The function first check to see if the data is in the form of a
          dictionary or a list. 

    Parameters
    ----------
    - The dataset in the form of a dictionary or a list. 
    '''
    workflow_ids = []

    # Check if data is a dictionary or a list
    if isinstance(data, dict):
        for key, value in data.items():
            if key == 'workflow_id':
                workflow_ids.append(value)
            else:
                workflow_ids.extend(extract_workflow_ids(value))
    elif isinstance(data, list):
        for item in data:
            workflow_ids.extend(extract_workflow_ids(item))

    return workflow_ids


def query_mongodb(workflow_id):
    '''
    Queries workflow ids against a MongoDB Database and returns
    the results as a dictionary.  

    Parameters
    ----------
    - A list of workflow ids to search against the database. 
    '''
    query_str = '{"workflow_run_id": "' + str(workflow_id) + '"}'

    # MongoDB export command using subprocess
    command = [
        "mongoexport",
        "--host", "workflow-metrics-db.gsi.oicr.on.ca",
        "--port", "27017",
        "--username", "workflow_metrics_ro",
        "--config", "/.mounts/labs/gsi/secrets/workflow-metrics-db.gsi_workflow_metrics_ro",
        "--db", "workflow_metrics",
        "--collection", "production_cromwell_workflow_metrics",
        "--jsonArray",
        "--query", query_str
    ]
    
    try:
        # Run mongoexport and capture the result
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
        
        # Parse the result from the command
        data = json.loads(result.stdout)
        
        # Return the data from MongoDB for further processing
        return data
    except subprocess.CalledProcessError as e:
        print(f"Error querying {workflow_id}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error while querying {workflow_id}: {e}")
        return None


def parse_json(data, workflow_metrics = None):
    '''
    Parses a dictionary to extract workflow metrics and compute the maximum wallclock_seconds for 'provisionFileOut'.
    
    Parameters
    -----------
    - The dictionary containing workflow metrics.
    - An empty dataframe to which new rows of extracted workflow metrics will be appended. 

    Returns
    -------
      A pandas dataframe containing workflow metrics, including the workflow name, start time, end time, 
      wallclock seconds, workflow run ID, and the maximum wallclock seconds for 'provisionFileOut'.
    '''
    unique_workflow = None
    provision_file_out_max_time = 0

    for workflow in data:
        workflow_name = workflow.get('workflow_name')
        wallclock_seconds = workflow.get('wallclock_seconds')

        if workflow_name != 'provisionFileOut':
            unique_workflow = workflow
        else:
            if wallclock_seconds > provision_file_out_max_time:
                provision_file_out_max_time = wallclock_seconds

    if unique_workflow:
        row = {
                'workflow_name': unique_workflow.get('workflow_name'),
                'start_time': unique_workflow.get('start_time'),
                'end_time': unique_workflow.get('end_time'),
                'wallclock_seconds': unique_workflow.get('wallclock_seconds'),
                'workflow_run_id': unique_workflow.get('workflow_run_id'),
                'max_provisionFileOut_wallclock_seconds': provision_file_out_max_time
            }
    
        if workflow_metrics is None:
            columns = ['workflow_name', 'start_time', 'end_time', 'wallclock_seconds', 'workflow_run_id', 'max_provisionFileOut_wallclock_seconds']
            workflow_metrics = pd.DataFrame(columns=columns)
        
        workflow_metrics = pd.concat([workflow_metrics, pd.DataFrame([row])], ignore_index=True)

    return workflow_metrics


def load_config(config_file):
    '''
    Loads the workflow run order and dependencies from a JSON file. 

    Parameters
    ----------
    - A path to the JSON workflow configuration file. 

    Returns
    -------
    - A List of workflows in the specified run order.
    - A Dictionary mapping workflow names to their dependencies.
    '''
    with open(config_file, 'r') as file:
        config = json.load(file)

    return config['workflow_run_order'], config['dependencies']


def add_arrows(metrics_df, dependencies):
    '''
    Creates a list of lines (arrows) linking the different workflows and showing the dependency between them.
    
    Parameters
    -----------
    - A pandas dataframe containing sorted workflow metrics ordered either by their start time or by their run order. 
    - A dictionary where keys are workflow names and values are lists of workflows that depend on the key workflow. 
    '''
    arrows = []
    for workflow, dependent_workflows in dependencies.items():
        for dep in dependent_workflows:
            workflow_rows = metrics_df[metrics_df['workflow_name'] == workflow]
            dep_rows = metrics_df[metrics_df['workflow_name'] == dep]
        
            for _, workflow_row in workflow_rows.iterrows():
                for _, dep_row in dep_rows.iterrows():
                    workflow_end_time = workflow_row['end_time']
                    dep_start_time = dep_row['start_time']
                
                    arrows.append(go.Scatter(
                        x=[workflow_end_time, dep_start_time], 
                        y=[workflow_row['workflow_name_id'], dep_row['workflow_name_id']], 
                        mode='lines',
                        line=dict(color='black', width=1, dash='dot'),
                        showlegend=False
                    ))

    return arrows


def update_axes(fig, metrics_df, run_order=None):
    '''
    Updates the axes of a given figure to display y axis values either based on run order or workflow start time. 
    
    Parameters
    -----------
    - The figure object to which the axes will be updated. 
    - A pandas dataframe containing sorted workflow metrics ordered either by their start time or by their run order. 
    - A list of workflow run orders.
    '''
    if run_order:
        tickvals = metrics_df['workflow_name_id'].unique()
        ticktext = [workflow for workflow in run_order for _ in range(len(metrics_df[metrics_df['workflow_name'] == workflow]))]
    else:
        tickvals = [workflow for workflow in metrics_df['workflow_name_id'].unique()]
        ticktext = [workflow.split('-')[0] for workflow in metrics_df['workflow_name_id']]

    fig.update_yaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor='black',
        gridcolor='lightgrey',
        autorange='reversed', 
        title="Workflow Name",
        tickmode='array',
        tickvals=tickvals,
        ticktext=ticktext
    )
    fig.update_xaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor='black',
        gridcolor='lightgrey',
        title="Time"
    )


def gantt_plot(workflow_metrics, config_file=None, png_file_1='wrt_gantt_v1.png', png_file_2='wrt_gantt_v2.png'):
    '''
    Generates two Gantt charts of workflow runtime. 

    Note: The function saves the Gantt charts as PNG files. The first chart (`png_file_1`) shows workflows sorted by their start times, 
    while the second chart (`png_file_2`) shows them sorted by their run order. 

    Parameters
    -----------
    - A pandas dataframe containing the workflow metrics. 
    - Two PNG files where the Gantt Charts will be saved. 
    '''
    generate_second_chart = False

    # Convert start_time and end_time to datetime format  
    workflow_metrics['start_time'] = pd.to_datetime(workflow_metrics['start_time'], errors = 'coerce')
    workflow_metrics['end_time'] = pd.to_datetime(workflow_metrics['end_time'], errors = 'coerce')
    
    # Sort based on start times and create a new column that concatenates workflow names and their run IDs.
    metrics_sorted = workflow_metrics.sort_values(by='start_time')
    metrics_sorted['workflow_name_id'] = metrics_sorted['workflow_name'] + '-' + metrics_sorted['workflow_run_id']

    fig_1 = px.timeline(metrics_sorted, 
                        x_start='start_time', 
                        x_end='end_time', 
                        y='workflow_name_id', 
                        color='workflow_run_id',
    )
    
    if config_file:
        #Generate the second plot and add dependency links only if workflow dependencies and run order are provided
        workflow_run_order, dependencies = load_config(config_file)
        generate_second_chart = True

        #Add dependency links to first plot
        arrows = add_arrows(metrics_sorted, dependencies)
        fig_1.add_traces(arrows)

        
    update_axes(fig_1, metrics_sorted)

    fig_1.update_layout(
        title='Gantt Chart of Workflow Runtime',
        title_x=0.5,
        plot_bgcolor='white',
        showlegend=False,
        annotations=[
                {
                    'text': "Sorted by run start time", 
                    'x': 0.45, 
                    'y': 1.06, 
                    'xref': 'paper',
                    'yref': 'paper',
                    'showarrow': False,
                    'font': {'size': 14, 'color': 'grey'},  
                    'align': 'center' 
                }
            ]
        )   
    fig_1.write_image(png_file_1)
    print(f"Workflow run metrics by start time saved to {png_file_1}")

    # Modify the 'y axis' values based on the run order and sort metrics based on this order
    if generate_second_chart:
        order_map = {workflow: idx for idx, workflow in enumerate(workflow_run_order)}
        metrics_sorted['run_order_y'] = metrics_sorted['workflow_name'].map(order_map)
        metrics_sorted_run_order = metrics_sorted.sort_values(by=['run_order_y'])

        fig_2 = px.timeline(metrics_sorted_run_order, 
                        x_start='start_time', 
                        x_end='end_time', 
                        y='workflow_name_id', 
                        color='workflow_run_id',
        )
    
        arrows = add_arrows(metrics_sorted_run_order, dependencies)
        fig_2.add_traces(arrows)

        update_axes(fig_2, metrics_sorted_run_order, workflow_run_order)

        fig_2.update_layout(
            title='Gantt Chart of Workflow Runtime',
            title_x=0.5,
            plot_bgcolor='white',
            showlegend=False,
            annotations=[
                {
                    'text': "Sorted by run order", 
                    'x': 0.45, 
                    'y': 1.06, 
                    'xref': 'paper',
                    'yref': 'paper',
                    'showarrow': False,
                    'font': {'size': 14, 'color': 'grey'},  
                    'align': 'center' 
                }
            ]
        )
        fig_2.write_image(png_file_2)
        print(f"Workflow run metrics by run order saved to {png_file_2}")


def generate_csv(workflow_metrics,  csv_file='workflow_report.csv'):
    '''
    Saves workflow metrics data generated from the parse_json function to a CSV file. 
    
    Note: If the file already exists, the function appends the data without writing the header again. 
    If the file does not exist, the data is written with the header.

    Parameters
    -----------
    - The pandas dataframe containing workflow metrics. 
    - An optional CSV filename/path where the metrics are to be stored. 
    '''
    # Check if CSV file already exists
    file_exists = os.path.isfile(csv_file)
        
    if file_exists:
        workflow_metrics.to_csv(csv_file, mode='a', header=False, index=False)
    else:
        # If the file does not exist, write the DataFrame with the header
        workflow_metrics.to_csv(csv_file, mode='w', header=True, index=False)
    
    print(f"Workflow run metrics saved to {csv_file}")

        

def process_json_file(json_file, config_file):
    '''
    Processes an input JSON file and calls the extract workflow ids, and 
    query mongodb functions. 

    Parameters
    ----------
    - Path to an input JSON file that needs to be processed to extract workflow ids.  
    '''
    # Check if file exists
    if not os.path.isfile(json_file):
        print(f"File {json_file} not found.")
        return

    # Load JSON data
    try:
        with open(json_file, 'r') as file:
            input_data = json.load(file)
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return

    # Extract workflow IDs
    workflow_ids = extract_workflow_ids(input_data)

    # Check if there are any workflow IDs extracted
    if not workflow_ids:
        print("No workflow IDs found.")
        return
    else:
        workflow_metrics = None
        for workflow_id in workflow_ids:
            print(f"Extracting workflow metrics for workflow_id: {workflow_id}")
            data = query_mongodb(workflow_id)
            if data:
                workflow_metrics = parse_json(data, workflow_metrics)

        # Generate gantt chart and CSV report if metrics are available
        if workflow_metrics is not None:
            gantt_plot(workflow_metrics, config_file)
            generate_csv(workflow_metrics)
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = 'Generate gantt charts for workflow metrics'
    )

    # Add the argument for the input JSON file and the workflow configuration file
    parser.add_argument(
        '-i', '--input',
        type = str,
        help = 'Path to the input JSON file containing workflow IDs',
        required = True
    )

    parser.add_argument(
        '--config',
        type = str,
        help = 'Path to the workflow configuration file containing run order and dependencies. [Optional]',
        required = False
    )

    # Add custom message to show usage
    parser.epilog = '''
    Example Usage: python3 workflow_rt.py -i /path/to/input/JSON 
    '''

    # Parse arguments
    args = parser.parse_args()
    json_file = args.input

    if len(vars(args)) == 0:
        parser.print_help()
        exit(0)
    else:
        process_json_file(json_file, config_file = args.config)