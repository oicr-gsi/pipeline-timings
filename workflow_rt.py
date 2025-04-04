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

    return list(set(workflow_ids))



def query_fpr(fp_path, workflow_ids):
    '''
    Queries workflow ids against File Provenance to extract sample names.

    Parameters
    ----------
    workflow_ids : List[str]
        A list of workflow ids to search against the FP report.

    Returns
    -------
    Optional List[str]
        A list of sample names, or None if an error occurred. 
    '''
    sname = []
    chunk_size = 10000
    for workflow_id in workflow_ids:
        try:
            for chunk in pd.read_csv(fp_path, sep='\t', compression='gzip', chunksize = chunk_size):
                filt_chunk = chunk[chunk['Workflow Run SWID'] == workflow_id]
                col = filt_chunk[['Root Sample Name', 'Workflow Run SWID']]
                sname.append(col)

        except subprocess.CalledProcessError as e:
            print(f"Error querying {workflow_id}: {e}")
            return None

        except Exception as e:
            print(f"Unexpected error while querying {workflow_id}: {e}")
            return None

    df = pd.concat(sname, ignore_index=True)
    col = ['sample_name', 'workflow_run_id']
    df.columns = col
    return df.drop_duplicates()



def query_mongodb(workflow_id):
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
    query_str = '{"workflow_run_id": "' + str(workflow_id) + '"}'
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
        data = json.loads(result.stdout)
        return data

    except subprocess.CalledProcessError as e:
        print(f"Error querying {workflow_id}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error while querying {workflow_id}: {e}")
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



def create_plot(df, fig_ht, fig_title, out_png, arrows=None, workflow_run_order=None):
    '''
    Creates a Gantt chart from the given metrics and saves it as a PNG file.

    Parameters
    ----------
    df : pd.DataFrame
        The sorted dataframe containing the workflow metrics.
    fig_ht : int
        The height of the plot.
    fig_title : str
        The title of the Gantt chart.
    out_png_file : str
        The path where the PNG file should be saved.
    
    Returns
    -------
    fig : plotly.graph_objects.Figure
        The figure object for the Gantt chart.
    ''' 
    fig = px.timeline(df, 
                      x_start='start_time', 
                      x_end='end_time', 
                      y='workflow_name_id', 
                      color='workflow_run_id')
    
    if arrows:
        fig.add_arrows(arrows)
    
    update_axes(fig, df, workflow_run_order)

    fig.update_layout(
        title=fig_title,
        title_x=0.5,
        plot_bgcolor='white',
        showlegend=False,
        height=fig_ht
    )
    
    return fig



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

    num_workflows = len(metrics_sorted['workflow_name_id'].unique())
    ht = max(400, num_workflows * 30) if num_workflows > 1 else 200

    unique_samples = workflow_metrics['sample_name'].nunique()
    
    arrows_1 = None
    arrows_2 = None
    workflow_run_order = None

    if config_file:
            #Generate the second plot and add dependency links only if workflow dependencies and run order are provided
            workflow_run_order, dependencies = load_config(config_file)
            generate_second_chart = True
            order_map = {workflow: idx for idx, workflow in enumerate(workflow_run_order)}
            metrics_sorted['run_order_y'] = metrics_sorted['workflow_name'].map(order_map)
            metrics_sorted_run_order = metrics_sorted.sort_values(by=['run_order_y'])
            
            #Add dependency links to first plot
            arrows_1 = add_arrows(metrics_sorted, dependencies) 
            arrows_2 = add_arrows(metrics_sorted_run_order, dependencies)

    if unique_samples == 1:
        sample_name = workflow_metrics['sample_name'].iloc[0]
        title_1 = f'Gantt Chart of Workflow Runtime (Sample: {sample_name})'
        fig_1 = create_plot(metrics_sorted, ht, title_1, png_file_1, arrows_1)
        fig_1.write_image(png_file_1)
        print(f"Workflow run metrics saved to {png_file_1}")

        if generate_second_chart:
            title_2 = f'Gantt Chart of Workflow Runtime (Sample: {sample_name})'
            fig_2 = create_plot(metrics_sorted_run_order, ht, title_2, png_file_2, arrows_2, workflow_run_order)
            fig_2.write_image(png_file_2)
            print(f"Workflow run metrics by run order saved to {png_file_2}")
    
    else:
        for sample in workflow_metrics['sample_name'].unique():
            sample_metrics = metrics_sorted[metrics_sorted['sample_name'] == sample]
            num_workflows = len(sample_metrics['workflow_name_id'].unique())
            ht = max(400, num_workflows * 30) if num_workflows > 1 else 200

            # Chart 1
            title_sample_1 = f'Gantt Chart of Workflow Runtime (Sample: {sample})'
            sample_png = f"{png_file_1.replace('.png', f'_{sample}.png')}"
            fig_1_sample = create_plot(sample_metrics, ht, title_sample_1, sample_png, arrows_1)
            fig_1_sample.write_image(sample_png)
            print(f"Workflow run metrics for sample {sample} saved to {sample_png}")

            
            if generate_second_chart:
                sample_metrics = metrics_sorted_run_order[metrics_sorted_run_order['sample_name'] == sample]
                num_workflows = len(sample_metrics['workflow_name_id'].unique())
                ht = max(400, num_workflows * 30) if num_workflows > 1 else 200

                # Chart 2
                title_sample_2 = f'Gantt Chart of Workflow Runtime (Sample: {sample})'
                sample_png = f"{png_file_2.replace('.png', f'_{sample}.png')}"
                fig_2_sample = create_plot(sample_metrics,ht, title_sample_2, sample_png, arrows_2, workflow_run_order)
                fig_2_sample.write_image(sample_png)
                print(f"Workflow run metrics by run order for sample {sample} saved to {sample_png}")



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
            workflow_ids = [
            line.strip() for line in file.readlines()
            if re.match(r'^[A-Za-z0-9\-]+$', line.strip()) 
        ]
        workflow_ids = list(set(workflow_ids))
    
    else:
        print(f"Error: The input must be either '.json' or '.txt' file")
        return

    # Check if there are any workflow IDs extracted
    if not workflow_ids:
        print("No workflow IDs found.")
        return
    
    fp_path = "/scratch2/groups/gsi/production/vidarr/vidarr_files_report_latest.tsv.gz"
    print("Extracting sample names for workflow_ids")
    df_sname = query_fpr(fp_path, workflow_ids)
    
    workflow_metrics = None
    print("Extracting workflow metrics for workflow_ids")
    for workflow_id in workflow_ids:
            data = query_mongodb(workflow_id)
            if data:
                workflow_metrics = parse_json(data, workflow_metrics)

    if workflow_metrics is not None and df_sname is not None:
        workflow_metrics = pd.merge(workflow_metrics, df_sname, left_on='workflow_run_id', right_on='workflow_run_id', how='left')
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
