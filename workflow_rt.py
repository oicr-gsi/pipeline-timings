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
    Extracts workflow ids from a dictionary or list and returns them as a list.

    Parameters
    ----------
    data : Union[dict, list]
        The dataset in the form of a dictionary or a list.

    Returns
    -------
    List[str]
        A list of workflow ids.
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


def query_mongodb(workflow_ids):
    '''
    Queries workflow ids against a MongoDB Database and returns the results as a dictionary.

    Parameters
    ----------
    workflow_ids : List[str]
        A list of workflow ids to search against the database.

    Returns
    -------
    Optional[Dict[str, dict]]
        A dictionary containing the results, or None if an error occurred. 
    '''
    query_str = '{"workflow_run_id": {"$in": ' + json.dumps(workflow_ids) + '}}'
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
        workflow_entry = json.loads(result.stdout)
        return {entry['workflow_run_id']: entry for entry in workflow_entry}
    except subprocess.CalledProcessError as e:
        print(f"Error querying workflows: {e}")
    except Exception as e:
        print(f"Unexpected error while querying workflows: {e}")
    
    return None


def parse_json(data, workflow_metrics=None):
    '''
    Parses a dictionary to extract workflow metrics and compute the maximum wallclock_seconds for 'provisionFileOut'.

    Parameters
    ----------
    data : List[dict]
        The dictionary containing workflow metrics.
    workflow_metrics : Optional[pd.DataFrame]
        An empty dataframe to which new rows of extracted workflow metrics will be appended.

    Returns
    -------
    pd.DataFrame
        A pandas dataframe containing workflow metrics.
    '''
    unique_workflow = None
    provision_file_out_max_time = 0

    for workflow in data:
        workflow_name = workflow.get('workflow_name')
        wallclock_seconds = workflow.get('wallclock_seconds')

        if workflow_name != 'provisionFileOut':
            unique_workflow = workflow
        else:
            provision_file_out_max_time = max(provision_file_out_max_time, wallclock_seconds)

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
    config_file : str
        A path to the JSON workflow configuration file.

    Returns
    -------
    Tuple[List[str], Dict[str, List[str]]]
        A list of workflows in the specified run order and a dictionary mapping workflow names to their dependencies.
    '''
    with open(config_file, 'r') as file:
        config = json.load(file)

    return config['workflow_run_order'], config['dependencies']


def add_arrows(metrics_df, dependencies):
    '''
    Creates a list of lines (arrows) linking the different workflows and showing the dependency between them.

    Parameters
    ----------
    metrics_df : pd.DataFrame
        A pandas dataframe containing sorted workflow metrics.
    dependencies : Dict[str, List[str]]
        A dictionary where keys are workflow names and values are lists of workflows that depend on the key workflow.

    Returns
    -------
    List[go.Scatter]
        A list of Plotly Scatter objects representing the arrows.
    '''
    arrows = []
    for workflow, dependent_workflows in dependencies.items():
        for dep in dependent_workflows:
            workflow_rows = metrics_df[metrics_df['workflow_name'] == workflow]
            dep_rows = metrics_df[metrics_df['workflow_name'] == dep]
        
            for _, workflow_row in workflow_rows.iterrows():
                for _, dep_row in dep_rows.iterrows():
                    arrows.append(go.Scatter(
                        x=[workflow_row['end_time'], dep_row['start_time']],
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
    ----------
    fig : go.Figure
        The figure object to which the axes will be updated.
    metrics_df : pd.DataFrame
        A pandas dataframe containing sorted workflow metrics.
    run_order : Optional[List[str]]
        A list of workflow run orders.
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
    Generates two Gantt charts of workflow runtime and saves them as PNG files.

    Parameters
    ----------
    workflow_metrics : pd.DataFrame
        A pandas dataframe containing the workflow metrics.
    config_file : Optional[str]
        Path to the workflow configuration file containing run order and dependencies. [Optional]
    png_file_1 : str
        Path to the first PNG file where the Gantt Chart will be saved.
    png_file_2 : str
        Path to the second PNG file where the Gantt Chart will be saved.
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


def generate_csv(workflow_metrics, csv_file='workflow_report.csv'):
    '''
    Saves workflow metrics data to a CSV file.

    Parameters
    ----------
    workflow_metrics : pd.DataFrame
        The pandas dataframe containing workflow metrics.
    csv_file : str
        The CSV filename/path where the metrics are to be stored. 
    '''
    # Check if CSV file already exists
    if os.path.isfile(csv_file):
        workflow_metrics.to_csv(csv_file, mode='a', header=False, index=False)
    else:
        workflow_metrics.to_csv(csv_file, mode='w', header=True, index=False)

    print(f"Workflow run metrics saved to {csv_file}")

        

def process_input_data(input_file, config_file=None):
    '''
    Processes an input JSON or Text file to retrieve workflow run ids.

    Parameters
    ----------
    input_file : str
        Path to an input JSON or Text file that needs to be processed to extract workflow ids.
    config_file : Optional[str]
        Path to the workflow configuration file containing run order and dependencies. [Optional]
    '''
    # Check if file exists
    if not os.path.isfile(input_file):
        print(f"File {input_file} not found.")
        return

    # Check if the input is a JSON or TXT file 
    if input_file.endswith('.json'):
        try:
            with open(input_file, 'r') as file:
                input_data = json.load(file)
            workflow_ids = extract_workflow_ids(input_data)
        except Exception as e:
            print(f"Error loading JSON file: {e}")
            return
        
    
    elif input_file.endswith('.txt'):
        with open(input_file, 'r') as file:
            workflow_ids = [line.strip() for line in file.readlines()]
    
    else:
        print(f"Error: The input must be either '.json' or '.txt' file")
        return

    # Check if there are any workflow IDs extracted
    if not workflow_ids:
        print("No workflow IDs found.")
        return
    
    workflow_metrics = None
    print(f"Extracting workflow metrics for workflow_ids")
    metrics_dict = query_mongodb(workflow_ids)
        
    if metrics_dict:
        workflow_metrics = None
        for workflow_id, metrics in metrics_dict.items():
            workflow_metrics = parse_json(metrics, workflow_metrics)

        if workflow_metrics is not None:
            gantt_plot(workflow_metrics, config_file)
            generate_csv(workflow_metrics)
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = 'Generate gantt charts from workflow run metrics'
    )

    # Add the argument for the input JSON file and the workflow configuration file
    parser.add_argument(
        '-i', '--input',
        type = str,
        help = 'Path to the input JSON or TXT file containing workflow IDs',
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
    Example Usage: pipeline-rt -i /path/to/input/JSON 
    OR
    pipeline-rt -i /path/to/input/TXT
    '''

    # Parse arguments
    args = parser.parse_args()
    input_file = args.input

    if len(vars(args)) == 0:
        parser.print_help()
        exit(0)
    else:
        process_input_data(input_file, config_file = args.config)