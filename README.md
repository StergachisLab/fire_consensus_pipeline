![Downloads](https://img.shields.io/github/downloads/StergachisLab/fire_consensus_pipeline/total)
![Last Commit](https://img.shields.io/github/last-commit/StergachisLab/fire_consensus_pipeline)
![Reproducible](https://img.shields.io/badge/reproducible-yes-brightgreen.svg)
[![ShellCheck](https://github.com/StergachisLab/fire_consensus_pipeline/actions/workflows/shellcheck.yml/badge.svg)](https://github.com/StergachisLab/fire_consensus_pipeline/actions/workflows/shellcheck.yml)

# FIRE peak consensus pipeline
---

## What it does:

Build consensus peaks from per-sample `*peaks.bed.gz` files, then recalculate per-sample actuation values from `*pileup.bed.gz` files against those consensus intervals.

## Workflow:

The workflow has three main steps:

1. Reduce per-sample peak files:
   - reads each sample's `peaks.bed.gz`
   - creates a temporary simplified representation used to build the pooled consensus

2. Build consensus peaks:
   - merges reduced peak information across samples
   - runs `ft mock-fire`
   - runs `ft call-peaks`
   - writes:
     - `consensus.intervals.bed`
     - `consensus_peak_ids.tsv`

3. Recalculate per-sample actuation on consensus intervals:
   - intersects each sample's pileup file with the consensus peaks
   - ranks overlapping rows
   - keeps the best row per consensus peak
   - writes one final output per sample


## How to run:

Pull the `peak_calling` branch of the fiberseq/fibertools-rs tool for this workflow:
```
git clone --branch peak_calling --single-branch https://github.com/fiberseq/fibertools-rs.git
git switch peak_calling
```

Create a manifest input file with 3 columns, so sample names and file paths are explicit.
- `sample`: sample name to use in output files
- `peaks`: path to the sample peaks BED.gz file
- `pileup`: path to the sample pileup BED.gz file

The pipeline supports three execution backends for the per-sample recalculation step:

- `local`
- `slurm`
- `pbs`

### Local:
```text
./fire_consensus_pipeline.sh \
  --manifest samples.tsv \
  --ft /path/to/ft \
  --runner local \
  --outdir results
```

### With SLURM:
```text
./fire_consensus_pipeline.sh \
  --manifest samples.input.tsv \
  --ft /path/to/ft \
  --runner slurm \
  --scheduler-config slurm.conf \
  --outdir results

-or-

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

### With PBS:
```text
./fire_consensus_pipeline.sh \
  --manifest samples.input.tsv \
  --ft /path/to/ft \
  --runner pbs \
  --scheduler-config pbs.conf \
  --outdir results

-or-

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

## Final outputs

## Output structure

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

Per-sample final files are written to:

```text
samples_recalc_actuation/<sample>.actuation.tsv
```
## Example output file

```tsv
peak	sample	chrom	start	end	score	coverage	fire_coverage	actuation	coverage_H1	fire_coverage_H1	coverage_H2	fire_coverage_H2
chr1_1000_1200	SAMPLE_A	chr1	1012	1188	42	100	12	0.12	48	6	52	6
chr1_2000_2200	SAMPLE_A	chr1	2015	2194	17	80	3	0.0375	35	1	45	2
chr2_500_800	SAMPLE_A	chr2	520	790	25	60	9	0.15	28	4	32	5
```
