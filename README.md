![Downloads](https://img.shields.io/github/downloads/StergachisLab/fire_consensus_pipeline/total)
![Last Commit](https://img.shields.io/github/last-commit/StergachisLab/fire_consensus_pipeline)
![Reproducible](https://img.shields.io/badge/reproducible-yes-brightgreen.svg)
# FIRE peak consensus pipeline
---

## Overview

This pipeline builds consensus peaks from per-sample `*peaks.bed.gz` files and then recalculates per-sample actuation values from `*pileup.bed.gz` files using the consensus intervals.

## Workflow

The pipeline consists of three main steps.

### 1. Reduce per-sample peak files

For each sample, the pipeline:

* Reads the `peaks.bed.gz` file.
* Creates a temporary, simplified representation of the peak data.
* Uses the simplified files to build the pooled consensus.

### 2. Build consensus peaks

The pipeline:

* Merges the reduced peak information across all samples.
* Runs `ft mock-fire`.
* Runs `ft call-peaks`.
* Writes the following consensus files:

  * `consensus.intervals.bed`
  * `consensus_peak_ids.tsv`

### 3. Recalculate per-sample actuation values

For each sample, the pipeline:

* Intersects the sample's pileup file with the consensus peaks.
* Ranks rows that overlap each consensus peak.
* Retains the best row for each consensus peak.
* Writes one final actuation file per sample.


## Installation

Create and activate the Conda environment:

```bash
conda env create -f environment.yml
conda activate fire-consensus-pipeline
```

If the version of `fibertools-rs` available through Conda does not yet include the features required by this pipeline, clone the `peak_calling` branch of `fiberseq/fibertools-rs`:

```bash
git clone --branch peak_calling --single-branch \
  https://github.com/fiberseq/fibertools-rs.git

cd fibertools-rs/
git branch --show-current
```

If the repository is not already on the `peak_calling` branch, switch to it:

```bash
git switch peak_calling
```

Build `fibertools-rs` according to its installation instructions, and then provide the path to the resulting `ft` executable using the `--ft` option.

## Input Manifest

Create a tab-separated manifest file with three columns so that sample names and input file paths are explicitly defined:

* `sample`: Sample name to use in output filenames.
* `peaks`: Path to the sample's peaks BED.gz file.
* `pileup`: Path to the sample's pileup BED.gz file.

For example:

```tsv
sample	peaks	pileup
SAMPLE_A	/path/to/SAMPLE_A.peaks.bed.gz	/path/to/SAMPLE_A.pileup.bed.gz
SAMPLE_B	/path/to/SAMPLE_B.peaks.bed.gz	/path/to/SAMPLE_B.pileup.bed.gz
```

## Resource Requirements

The entire pipeline is launched with a single command, but its resource allocation differs across the three workflow steps. 

The command itself runs the first two steps on the node where it was started. This node typically requires approximately 100 GB of memory and 8 CPUs, although the exact requirements depend on the number of samples and the sizes of the input files. Run the pipeline command either on an interactive node or within a submitted batch job that has sufficient resources for these steps.

After the first two steps complete successfully, the pipeline automatically begins the third step. With the local runner, the per-sample recalculations run locally (slowest). With the slurm or pbs runner, the pipeline submits one scheduler job per sample using the resources specified in the scheduler configuration file or command-line options.

The --cpus, --mem, and --time options passed to fire_consensus_pipeline.sh apply only to the per-sample jobs in the third step. They do not allocate resources for the node running the main pipeline command.

The required resources depend on:

* The number of samples being processed.
* The sizes of the input files.
* Whether the pipeline is run locally or with a job scheduler.
* Whether sufficient scheduler resources are available to run the third step in parallel.

Adjust the requested resources as needed for your dataset and computing environment.

For example, on a SLURM cluster:

```bash
salloc \
  --partition compute-ultramem \
  --account stergachislab \
  --time 70:00:00 \
  --mem 100G \
  --cpus-per-task 8

conda activate fire-consensus-pipeline

PATH_TO_FT=/mmfs1/gscratch/stergachislab/mvollger/projects/dev-fibertools-rs/target/release/ft

./fire_consensus_pipeline.sh \
  --manifest manifest.tsv \
  --ft "$PATH_TO_FT" \
  --runner slurm \
  --scheduler-config slurm.conf \
  --account stergachislab \
  --partition ckpt \
  --cpus 4 \
  --mem 32G \
  --time 08:00:00 \
  --outdir results
```

The `--cpus`, `--mem`, and `--time` options in the pipeline command apply to the per-sample jobs created during the third step. They do not control the resources allocated to the first two steps.

## Per-Sample Parallel Jobs

The third step performs the per-sample recalculation and can run multiple jobs in parallel.

Each individual job has relatively modest CPU and memory requirements. However, your computing environment must have enough available resources to scale to the number of samples listed in the manifest.

When using SLURM or PBS, the per-sample jobs are submitted automatically after the first two steps complete successfully.

Scheduler-specific settings for these jobs can be provided through a scheduler configuration file or as command-line arguments.

The pipeline supports the following execution backends:

* `local`
* `slurm`
* `pbs`

## Running Locally

```bash
./fire_consensus_pipeline.sh \
  --manifest samples.tsv \
  --ft /path/to/ft \
  --runner local \
  --outdir results
```

## Running with SLURM

### Using a scheduler configuration file

```bash
./fire_consensus_pipeline.sh \
  --manifest samples.input.tsv \
  --ft /path/to/ft \
  --runner slurm \
  --scheduler-config slurm.conf \
  --outdir results
```

### Providing scheduler settings directly

```bash
./fire_consensus_pipeline.sh \
  --manifest samples.input.tsv \
  --ft /path/to/ft \
  --runner slurm \
  --scheduler-config slurm.conf \
  --account myaccount \
  --partition compute \
  --cpus 4 \
  --mem 32G \
  --time 08:00:00 \
  --outdir results
```

## Running with PBS

### Using a scheduler configuration file

```bash
./fire_consensus_pipeline.sh \
  --manifest samples.input.tsv \
  --ft /path/to/ft \
  --runner pbs \
  --scheduler-config pbs.conf \
  --outdir results
```

### Providing scheduler settings directly

```bash
./fire_consensus_pipeline.sh \
  --manifest samples.input.tsv \
  --ft /path/to/ft \
  --runner pbs \
  --scheduler-config pbs.conf \
  --queue batch \
  --cpus 4 \
  --mem 32gb \
  --time 08:00:00 \
  --outdir results
```

## Output Structure

The output directory has the following structure:

```text
fire_consensus_out/
├── consensus.intervals.bed
├── consensus_peak_ids.tsv
├── merged.4col.bed.gz
├── output.mock_bam
├── output.mock_bam.bai
├── output.peaks
├── logs/
├── sample2consensus.sh
├── samples_recalc_actuation/
│   ├── SAMPLE_A.actuation.tsv
│   └── SAMPLE_B.actuation.tsv
└── tmp_consensus_inputs/
```

The final per-sample files are written to:

```text
samples_recalc_actuation/<sample>.actuation.tsv
```

## Example Output File

```tsv
peak	sample	chrom	start	end	score	coverage	fire_coverage	actuation	coverage_H1	fire_coverage_H1	coverage_H2	fire_coverage_H2
chr1_1000_1200	SAMPLE_A	chr1	1012	1188	42	100	12	0.12	48	6	52	6
chr1_2000_2200	SAMPLE_A	chr1	2015	2194	17	80	3	0.0375	35	1	45	2
chr2_500_800	SAMPLE_A	chr2	520	790	25	60	9	0.15	28	4	32	5
```

After all per-sample jobs have completed, every sample should have exactly the same number of data rows as `consensus.intervals.bed`, including unmatched peaks represented by NA. 
We recommend merging all per-sample files for any comparative downstream analysis. 


```
awk 'FNR == 1 && NR != 1 {next} {print}' \
  samples_recalc_actuation/*.actuation.tsv \
  > all_samples.actuation.tsv
```
