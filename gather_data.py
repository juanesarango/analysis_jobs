#%%
from os.path import join
import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


ROOT = os.getcwd()

ROOT = join(ROOT, 'fixed_stats')

PIPELINES = [
    (1, 'BATTENBERG'),
    (2, 'BRASS'),
    (3, 'CAVEMAN'),
    (4, 'CNVKIT'),
    (5, 'FLT3ITD'),
    (6, 'PINDEL'),
    (7, 'RNACALLER'),
    (8, 'MERGE_TABLES'),
    (9, 'ANNOT_CAVEMAN'),
    (10, 'ANNOT_PINDEL'),
    (11, 'ANNOT_CAVEMAN'),
    (12, 'ANNOT_PINDEL'),
    (13, 'PTD'),
    (14, 'QC_DATA'),
    (15, 'FACETS'),
    (16, 'RNAFUSIONS'),
    (17, 'CONPAIR')
]


def get_step_name(log_file):
    file_name = log_file.split('/')[-1]
    job_name = file_name.split('.')[0]
    step_name = job_name.split('_')
    return step_name[1] if len(step_name) > 1 else None


def aggregate_jobs(head_job=True):
    frames = []

    for pipeline_id, _ in PIPELINES:

        if head_job:
            stats_log = join(ROOT, f'stats_{pipeline_id}_head.txt')
        else:
            stats_log = join(ROOT, f'stats_{pipeline_id}.txt')

        if not os.path.isfile(stats_log): continue

        df = pd.read_csv(stats_log, sep="\t", header=None, names=[
            "Analysis pk",
            "Technique",
            "BAM Size kb",
            "Pipeline",
            "LogFile",
            "Total Requested Memory",
            "Delta Memory",
            "Max Swap",
            "Run Time",
            "Step Name",
        ])

        if not df.empty:
            df['Step Name'] = df.apply(
                lambda row: get_step_name(row["LogFile"]), axis=1
            )
            frames.append(df)

    # Concatenate all pipelines dataframes
    data = pd.concat(frames)

    # Aggregate data by Time and Memory
    data_time = data[data['Run Time'].notnull()]
    data_time_grouped = data_time.groupby(["Pipeline", "Technique", "Step Name"]).agg({
        'Run Time': [
            'count',
            'mean',
            'std',
            'min',
            'max'
        ]
    }).round(1)

    data_memory = data[data['Total Requested Memory'].notnull() & data['Max Swap'].notnull()]
    data_memory_grouped = data_memory.groupby(["Pipeline", "Technique", "Step Name"]).agg({
        'Step Name': [
            'count'
        ],
        'Total Requested Memory': [
            'mean',
            'std',
            'min',
            'max'
        ],
        'Max Swap': [
            'mean',
            'std',
            'min',
            'max'
        ],
    }).round(1)

    # Otput to terminal
    print("HEAD JOB RUNTIME" if head_job else "STEP JOBS RUNTIME")
    print(data_time_grouped)
    data_time_grouped.to_csv(join(ROOT, 'grouped_time_stats.txt'), sep="\t")

    print("HEAD JOB MEMORY" if head_job else "STEP JOBS MEMORY")
    print(data_memory_grouped)
    data_memory_grouped.to_csv(join(ROOT, 'grouped_memory_stats.txt'), sep="\t")

    # Write to files
    if head_job:
        data_time_grouped.to_csv('stats_time_head.txt', sep='\t')
        data_memory_grouped.to_csv('stats_memory_head.txt', sep='\t')
    else:
        data_time_grouped.to_csv('stats_time.txt', sep='\t')
        data_memory_grouped.to_csv('stats_memory.txt', sep='\t')

    return data_time

data_total = aggregate_jobs(True)
data_jobs = aggregate_jobs(False)

#%%

def plot_df(data, filters, plot_var):

    var, ylabel, xlabel = plot_var
    for column, value in filters:
        data = data[data[column] == value]
    data = data[var]

    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(10, 4))
    title = f"{' '.join([ name for _, name in filters])} - {var}"

    ax1 = data.plot(
        style=['.'],
        kind='hist',
        bins=100,
        title=title,
        ax=axes[0],
    )
    ax1.set_xlabel(ylabel)
    ax1.set_ylabel(xlabel)

    ax2 = data.plot(
        style=['.'],
        title=title,
        ax=axes[1],
        color='darkorange'
    )
    ax2.set_xlabel(ylabel)
    ax2.set_ylabel(xlabel)
    plt.tight_layout()


def plot_pipelines(data):
    pipelines = data['Pipeline'].unique()
    steps = data['Step Name'].unique()

    for step in steps[:]:
        filters = [
            ("Pipeline", pipelines[0]),
            ("Technique", "WGS"),
            ("Step Name", step)
        ]

        plot_var = ("Run Time", "time (sec)", "# Jobs")
        plot_df(data, filters, plot_var)


plot_pipelines(data_jobs)
#%%

# PRINT TOTAL TABLE
data_total = aggregate_jobs(True)

filters = [
    ("Pipeline", pipelines[0]),
    ("Technique", "WGS"),
]

# PRINT TOTAL GRAPHS
plot_var = ("Run Time", "time (sec)", "# Jobs")
plot_df(data_total, filters, plot_var)


#%%

data_jobs.to_csv(join(ROOT, 'time_stats.txt'), sep="\t")
