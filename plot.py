import os
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import sys

def update_axes(fig, df):
    tickvals = df['workflow_name_id'].unique()
    ticktext = [w.split('-')[0] for w in tickvals]

    fig.update_yaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor='black',
        gridcolor='lightgrey',
        showgrid=True,
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
        showgrid=True,
        title="Time"
    )

def create_gantt(df, sample_name, html_out, threshold=0.5):
    fig = make_subplots(rows=1, cols=1)

    t0 = df['start_time'].min()
    df['start_hr'] = (df['start_time'] - t0).dt.total_seconds() / 3600
    df['end_hr'] = (df['end_time'] - t0).dt.total_seconds() / 3600

    # Sort and compress idle time
    df = df.sort_values(by='start_hr').copy()
    shift = 0
    last_time = df.iloc[0]['end_hr']

    compressed_start = []
    compressed_end = []

    for _, row in df.iterrows():
        gap = row['start_hr'] - last_time
        if gap > threshold:
            shift += gap - threshold

        compressed_s = row['start_hr'] - shift
        compressed_e = row['end_hr'] - shift

        compressed_start.append(compressed_s)
        compressed_end.append(compressed_e)

        last_time = row['end_hr']

    df['rel_start'] = compressed_start
    df['rel_end'] = compressed_end
    df['duration'] = df['rel_end'] - df['rel_start']
    
    # Plotting the Gantt chart
    for _, row in df.iterrows():
        hover_text = (
            f"Workflow Name: {row['workflow_name']}<br>"
            f"Workflow_ID: {row['workflow_run_id']}<br>"
            f"Start: {row['start_time'].strftime('%Y-%m-%d %H:%M')}<br>"
            f"End: {row['end_time'].strftime('%Y-%m-%d %H:%M')}<br>"
            f"Duration: {row['duration']:.2f} hr"
        )

        fig.add_trace(go.Bar(
            x=[row['duration']],
            y=[row['workflow_name_id']],
            base=[row['rel_start']],
            orientation='h',
            width=0.5,
            marker=dict(color='#4682B4'),
            hoverinfo='text',
            hovertext=hover_text
        ))

    update_axes(fig, df)

    fig.update_layout(
        title=dict(text=f"Gantt Chart of Workflow Runtime (Sample: {sample_name})", x=0.5, font=dict(size=16)),
        plot_bgcolor='white',
        paper_bgcolor='white',
        showlegend=False,
        height=max(400, len(df) * 30),
        xaxis_title="Hours since start",
        yaxis_title="Workflow Name",
        hoverlabel=dict(
            bgcolor="white",
            font_size=10,
            font_family="Arial",
            font_color="black"
        )
    )

    fig.write_html(html_out)
    print(f"[✓] Saved: {html_out}")


def gantt(csv_file):
    df = pd.read_csv(csv_file)

    if df.empty or 'workflow_name' not in df or 'workflow_run_id' not in df:
        print("Error: Invalid or empty CSV file.")
        sys.exit(1)

    # Parse timestamps
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])
    df['workflow_name_id'] = df['workflow_name'] + '-' + df['workflow_run_id']

    if 'sample_name' not in df.columns:
        print("Error: CSV file must contain a 'sample_name' column.")
        sys.exit(1)

    # Plot for each sample
    for sample in df['sample_name'].unique():
        sample_df = df[df['sample_name'] == sample].copy()
        sample_df.sort_values(by='start_time', inplace=True)
        html_out = csv_file.replace('.csv', f'_gantt_{sample}.html')
        create_gantt(sample_df, sample, html_out, threshold=0.5)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python basic_plot.py <workflow_metrics.csv>")
        sys.exit(1)

    csv_file = sys.argv[1]
    if not os.path.exists(csv_file):
        print(f"Error: File '{csv_file}' does not exist.")
        sys.exit(1)

    gantt(csv_file)
