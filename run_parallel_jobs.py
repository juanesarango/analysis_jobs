import subprocess


PIPELINES = [
    # (1, 'BATTENBERG'),
    # (2, 'BRASS'),
    (3, 'CAVEMAN'),
    # (4, 'CNVKIT'),
    # (5, 'FLT3ITD'),
    (6, 'PINDEL'),
    # (7, 'RNACALLER'),
    # (8, 'MERGE_TABLES'),
    # (9, 'ANNOT_CAVEMAN'),
    # (10, 'ANNOT_PINDEL'),
    # (11, 'ANNOT_CAVEMAN'),
    # (12, 'ANNOT_PINDEL'),
    # (13, 'PTD'),
    # (14, 'QC_DATA'),
    # (15, 'FACETS'),
    # (16, 'RNAFUSIONS'),
    # (17, 'CONPAIR')
]

if __name__ == "__main__":
    """
    Script to call
        bsub
            -We 60
            -n 8
            -M 20
            -R "rusage[mem=256]"
            -o logs/collect-stats-pipeline-{pipeline_id}.%J.log
                'python job_head_collection.py {pipeline_id}'
    """
    for pipeline_id, _ in PIPELINES:
        cmd = [
            "bsub",
            "-We", "60",
            "-n", "8",
            "-M", "20",
            "-R", "rusage[mem=256]",
            '-o', f'logs/collect-stats-pipeline-{pipeline_id}.%J.log',
            f'python job_head_collection.py {pipeline_id}'
        ]
        print(' '.join(cmd))
        output = subprocess.check_output(cmd)
        print(output)


