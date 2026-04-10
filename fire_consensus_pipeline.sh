#!/usr/bin/env bash
set -euo pipefail

show_help() {
  cat <<'EOF'
Usage:
  fire_consensus_pipeline.sh --manifest samples.tsv --ft /path/to/ft [options]

Description:
  Build consensus peaks from per-sample peak BED.gz files and recalculate
  per-sample actuation values from pileup BED.gz files against the consensus
  intervals.

Required input:
  --manifest FILE
      Tab-separated manifest with header:
        sample    peaks    pileup

      Columns:
        sample   Sample identifier used in outputs
        peaks    Path to sample peaks BED.gz file
        pileup   Path to sample pileup BED.gz file

Options:
  -m, --manifest FILE
      Input manifest TSV.

  -o, --outdir DIR
      Output directory. Default: fire_consensus_out

  -f, --ft PATH
      Path to ft executable.

  --runner NAME
      Execution backend for per-sample recalculation.
      Choices:
        local
        slurm
        pbs
      Default: local

  -s, --scheduler-config FILE
      Shell config file with runner settings.
      Default: ./scheduler.conf

  --stage NAME
      One of:
        all
        reduce-peaks
        consensus
        submit-sample-reduction
      Default: all

  -j, --jobs N
      Parallel jobs for local processing. Default: 8

  --keep-temp
      Keep temporary files used to build consensus.
      By default they are removed after consensus generation.

  --logs-dir DIR
      Directory for logs. Default: <outdir>/logs

  --account NAME
      Override scheduler account/project from config where applicable

  --partition NAME
      Override SLURM partition from config

  --queue NAME
      Override PBS queue from config

  --cpus N
      Override CPU count from config

  --mem MEM
      Override memory from config

  --time TIME
      Override walltime from config

  --dry-run
      Print commands without running them.

  -h, --help
      Show this help.

Example:
  fire_consensus_pipeline.sh \
    --manifest samples.tsv \
    --ft /dev-fibertools-rs/target/release/ft \
    --runner local \
    --outdir results

Manifest example:
  sample    peaks    pileup
  SAMPLE_A  /data/SAMPLE_A-peaks.bed.gz   /data/SAMPLE_A-pileup.bed.gz
  SAMPLE_B  /data/SAMPLE_B-peaks.bed.gz   /data/SAMPLE_B-pileup.bed.gz

Final per-sample outputs:
  samples_recalc_actuation/<sample>.actuation.tsv

Columns:
  peak    sample    chrom    start    end    score    coverage    fire_coverage    actuation    coverage_H1    fire_coverage_H1    coverage_H2    fire_coverage_H2

Selection of best overlap per consensus peak:
  For each sample, overlapping rows are ranked and only the top row per peak is kept.

  Sort priority is:
    1. peak id ascending
    2. score descending
    3. actuation descending
    4. fire_coverage descending
    5. coverage descending

  After sorting, the first row for each peak is retained.

Expected pileup column order:
  1.  #chrom
  2.  start
  3.  end
  4.  coverage
  5.  fire_coverage
  6.  score
  7.  nuc_coverage
  8.  msp_coverage
  9.  coverage_H1
  10. fire_coverage_H1
  11. score_H1
  12. nuc_coverage_H1
  13. msp_coverage_H1
  14. coverage_H2
  15. fire_coverage_H2
  16. score_H2
  17. nuc_coverage_H2
  18. msp_coverage_H2
  19. is_local_max
  20. FDR
  21. log_FDR

Scheduler config file format:
  Plain shell variable assignments.

  For local mode:
    LOCAL_JOBS="8"

  For SLURM mode:
    SLURM_ACCOUNT="myaccount"
    SLURM_PARTITION="compute"
    SLURM_CPUS_PER_TASK="2"
    SLURM_MEM="20G"
    SLURM_TIME="12:00:00"
    SLURM_EXTRA_ARGS=""

  For PBS mode:
    PBS_ACCOUNT="myproject"
    PBS_QUEUE="batch"
    PBS_NCPUS="2"
    PBS_MEM="20gb"
    PBS_WALLTIME="12:00:00"
    PBS_EXTRA_ARGS=""

EOF
}

log() {
  echo "[$(date '+%F %T')] $*" >&2
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

run_cmd() {
  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "[DRY-RUN] $*"
  else
    eval "$@"
  fi
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

abs_path() {
  local p="$1"
  python3 - <<'PY' "$p"
import os, sys
print(os.path.abspath(sys.argv[1]))
PY
}

validate_manifest() {
  local manifest="$1"

  awk 'BEGIN{FS=OFS="\t"}
    NR==1 {
      if ($1 != "sample" || $2 != "peaks" || $3 != "pileup") {
        print "Manifest header must be: sample<TAB>peaks<TAB>pileup" > "/dev/stderr"
        exit 1
      }
      next
    }
    NF < 3 {
      print "Invalid manifest line " NR ": expected at least 3 tab-separated columns" > "/dev/stderr"
      exit 1
    }
    $1 == "" || $2 == "" || $3 == "" {
      print "Invalid manifest line " NR ": sample, peaks, and pileup must all be non-empty" > "/dev/stderr"
      exit 1
    }
  ' "$manifest"

  while IFS=$'\t' read -r sample peaks pileup _rest; do
    [[ "$sample" == "sample" ]] && continue
    [[ -n "$sample" ]] || die "Empty sample in manifest"
    [[ -f "$peaks" ]] || die "Peaks file not found for sample $sample: $peaks"
    [[ -f "$pileup" ]] || die "Pileup file not found for sample $sample: $pileup"
  done < "$manifest"
}

write_sample2consensus_script() {
  local script_path="$1"

  cat > "$script_path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

sample_to_consensus_bedtools() {
    local sample_file="$1"
    local sample_name="$2"
    local consensus_bed="$3"
    local out_tsv="$4"

    {
      printf "peak\tsample\tchrom\tstart\tend\tscore\tcoverage\tfire_coverage\tactuation\tcoverage_H1\tfire_coverage_H1\tcoverage_H2\tfire_coverage_H2\n"

      gzip -dc "$sample_file" \
      | awk 'BEGIN{FS=OFS="\t"}
             NR==1 && $1 ~ /^#/ {next}
             {
               # Expected pileup columns:
               # 1  chrom
               # 2  start
               # 3  end
               # 4  coverage
               # 5  fire_coverage
               # 6  score
               # 7  nuc_coverage
               # 8  msp_coverage
               # 9  coverage_H1
               # 10 fire_coverage_H1
               # 11 score_H1
               # 12 nuc_coverage_H1
               # 13 msp_coverage_H1
               # 14 coverage_H2
               # 15 fire_coverage_H2
               # 16 score_H2
               # 17 nuc_coverage_H2
               # 18 msp_coverage_H2
               # 19 is_local_max
               # 20 FDR
               # 21 log_FDR
               print $1,$2,$3,$4,$5,$6,$9,$10,$14,$15
             }' \
      | bedtools intersect -a "$consensus_bed" -b - -wa -wb -sorted \
      | awk 'BEGIN{FS=OFS="\t"}
             {
               # A = consensus.intervals.bed
               # 1  chr
               # 2  start
               # 3  end
               # 4  peak_id
               #
               # B = selected pileup columns
               # 5  chrom
               # 6  start
               # 7  end
               # 8  coverage
               # 9  fire_coverage
               # 10 score
               # 11 coverage_H1
               # 12 fire_coverage_H1
               # 13 coverage_H2
               # 14 fire_coverage_H2

               peak_id = $4
               chrom = $5
               start = $6
               end = $7
               cov = $8 + 0
               fire = $9 + 0
               score = $10 + 0
               cov_h1 = $11 + 0
               fire_h1 = $12 + 0
               cov_h2 = $13 + 0
               fire_h2 = $14 + 0
               act = (cov > 0 ? fire / cov : "NA")

               print peak_id, chrom, start, end, score, cov, fire, act, cov_h1, fire_h1, cov_h2, fire_h2
             }' \
      | sort -t $'\t' -k1,1 -k5,5nr -k8,8gr -k7,7nr -k6,6nr \
      | awk -v sample="$sample_name" 'BEGIN{FS=OFS="\t"}
             !seen[$1]++ {
               print $1, sample, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
             }'
    } > "$out_tsv"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  if [[ "$#" -ne 4 ]]; then
    echo "Usage: $0 <sample_file.bed.gz> <sample_name> <consensus_bed> <out_tsv>" >&2
    exit 1
  fi
  sample_to_consensus_bedtools "$1" "$2" "$3" "$4"
fi
EOF

  chmod +x "$script_path"
}

load_scheduler_config() {
  if [[ -f "$SCHEDULER_CONFIG" ]]; then
    # shellcheck disable=SC1090
    source "$SCHEDULER_CONFIG"
  fi

  LOCAL_JOBS="${LOCAL_JOBS:-$JOBS}"

  SLURM_ACCOUNT="${CLI_ACCOUNT:-${SLURM_ACCOUNT:-}}"
  SLURM_PARTITION="${CLI_PARTITION:-${SLURM_PARTITION:-}}"
  SLURM_CPUS_PER_TASK="${CLI_CPUS:-${SLURM_CPUS_PER_TASK:-2}}"
  SLURM_MEM="${CLI_MEM:-${SLURM_MEM:-20G}}"
  SLURM_TIME="${CLI_TIME:-${SLURM_TIME:-12:00:00}}"
  SLURM_EXTRA_ARGS="${SLURM_EXTRA_ARGS:-}"

  PBS_ACCOUNT="${CLI_ACCOUNT:-${PBS_ACCOUNT:-}}"
  PBS_QUEUE="${CLI_QUEUE:-${PBS_QUEUE:-}}"
  PBS_NCPUS="${CLI_CPUS:-${PBS_NCPUS:-2}}"
  PBS_MEM="${CLI_MEM:-${PBS_MEM:-20gb}}"
  PBS_WALLTIME="${CLI_TIME:-${PBS_WALLTIME:-12:00:00}}"
  PBS_EXTRA_ARGS="${PBS_EXTRA_ARGS:-}"
}

validate_runner_settings() {
  load_scheduler_config

  case "$RUNNER" in
    local)
      ;;
    slurm)
      require_cmd sbatch
      [[ -n "${SLURM_ACCOUNT}" ]] || die "SLURM_ACCOUNT not set. Use --account or provide it in $SCHEDULER_CONFIG"
      [[ -n "${SLURM_PARTITION}" ]] || die "SLURM_PARTITION not set. Use --partition or provide it in $SCHEDULER_CONFIG"
      ;;
    pbs)
      require_cmd qsub
      [[ -n "${PBS_QUEUE}" ]] || die "PBS_QUEUE not set. Use --queue or provide it in $SCHEDULER_CONFIG"
      ;;
    *)
      die "Unsupported runner: $RUNNER"
      ;;
  esac
}

reduce_peaks_stage() {
  mkdir -p "$REDUCED_DIR"

  log "Reducing per-sample peak files into temporary TSV parts"

  if [[ "$DRY_RUN" == "1" ]]; then
    awk 'BEGIN{FS=OFS="\t"} NR>1 {print "[DRY-RUN] would process sample=" $1 " peaks=" $2}' "$MANIFEST"
    return
  fi

  local manifest_pairs
  manifest_pairs=$(mktemp)

  awk 'BEGIN{FS=OFS="\t"} NR>1 {print $1 "\t" $2}' "$MANIFEST" > "$manifest_pairs"

  while IFS=$'\t' read -r sample peaks; do
    (
      out="$REDUCED_DIR/${sample}.peaks.reduced.tsv"
      gzip -cd -- "$peaks" \
        | awk -v s="$sample" 'BEGIN{OFS="\t"} !/^#/ && NF>=6 {print $1,$2,$3,$6,s}' \
        > "$out"
    ) &
    while [[ "$(jobs -r | wc -l)" -ge "$JOBS" ]]; do
      wait -n
    done
  done < "$manifest_pairs"

  wait
  rm -f "$manifest_pairs"
}

consensus_stage() {
  shopt -s nullglob
  local reduced_files=( "$REDUCED_DIR"/*.peaks.reduced.tsv )
  shopt -u nullglob

  [[ ${#reduced_files[@]} -gt 0 ]] || die "No temporary reduced peak TSVs found in $REDUCED_DIR"
  [[ -x "$FT_PATH" ]] || die "ft executable not found or not executable: $FT_PATH"

  require_cmd bgzip
  require_cmd samtools

  log "Building merged 4-column BED"
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[DRY-RUN] cat $REDUCED_DIR/*.peaks.reduced.tsv | awk ... | bgzip -@ $JOBS > $MERGED_BED_GZ"
  else
    cat "$REDUCED_DIR"/*.peaks.reduced.tsv \
      | awk 'BEGIN{OFS="\t"} {print $1,$2,$3,$4"_"$1}' \
      | bgzip -@ "$JOBS" > "$MERGED_BED_GZ"
  fi

  log "Running ft mock-fire and call-peaks"
  run_cmd "\"$FT_PATH\" mock-fire \"$MERGED_BED_GZ\" | samtools sort -o \"$MOCK_BAM\" --write-index"
  run_cmd "\"$FT_PATH\" call-peaks \"$MOCK_BAM\" --min-fire-coverage 1 --min-fire-frac 0.01 > \"$OUTPUT_PEAKS\""

  log "Creating consensus BED and peak ID list"
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[DRY-RUN] awk to create $CONSENSUS_BED and $CONSENSUS_PEAK_IDS"
  else
    awk 'BEGIN{OFS="\t"}
      NR==1 {next}
      {
        peak_id = $1 "_" $2 "_" $3
        print $1, $2, $3, peak_id
      }' "$OUTPUT_PEAKS" > "$CONSENSUS_BED"

    awk 'BEGIN{OFS="\t"} NR>1 {print $1"_"$2"_"$3}' "$OUTPUT_PEAKS" > "$CONSENSUS_PEAK_IDS"
  fi

  if [[ "$KEEP_TEMP" != "1" ]]; then
    rm -rf "$REDUCED_DIR"
  fi
}

submit_local_job() {
  local pileup="$1"
  local sample="$2"
  local out="$3"

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "[DRY-RUN] local: $SAMPLE2CONS_SCRIPT $pileup $sample $CONSENSUS_BED $out"
    return
  fi

  "$SAMPLE2CONS_SCRIPT" "$pileup" "$sample" "$CONSENSUS_BED" "$out" \
    > "$LOGS_DIR/${sample}.local.out" \
    2> "$LOGS_DIR/${sample}.local.err"
}

submit_slurm_job() {
  local pileup="$1"
  local sample="$2"
  local out="$3"
  local cmd

  cmd=$(printf '%q ' \
    "$SAMPLE2CONS_SCRIPT" \
    "$pileup" \
    "$sample" \
    "$CONSENSUS_BED" \
    "$out")

  if [[ "$DRY_RUN" == "1" ]]; then
    cat <<EOF
[DRY-RUN] sbatch \
  --job-name=fire_${sample} \
  --account=${SLURM_ACCOUNT} \
  --partition=${SLURM_PARTITION} \
  --output=${LOGS_DIR}/${sample}.%j.out \
  --error=${LOGS_DIR}/${sample}.%j.err \
  --cpus-per-task=${SLURM_CPUS_PER_TASK} \
  --mem=${SLURM_MEM} \
  --time=${SLURM_TIME} \
  ${SLURM_EXTRA_ARGS} \
  --wrap "$cmd"
EOF
  else
    # shellcheck disable=SC2086
    sbatch \
      --job-name="fire_${sample}" \
      --account="${SLURM_ACCOUNT}" \
      --partition="${SLURM_PARTITION}" \
      --output="${LOGS_DIR}/${sample}.%j.out" \
      --error="${LOGS_DIR}/${sample}.%j.err" \
      --cpus-per-task="${SLURM_CPUS_PER_TASK}" \
      --mem="${SLURM_MEM}" \
      --time="${SLURM_TIME}" \
      ${SLURM_EXTRA_ARGS} \
      --wrap "$cmd"
  fi
}

submit_pbs_job() {
  local pileup="$1"
  local sample="$2"
  local out="$3"
  local job_script

  job_script="$OUTDIR/pbs_job_${sample}.sh"

  cat > "$job_script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd $(printf '%q' "$PWD")
$(printf '%q ' "$SAMPLE2CONS_SCRIPT" "$pileup" "$sample" "$CONSENSUS_BED" "$out")
EOF
  chmod +x "$job_script"

  if [[ "$DRY_RUN" == "1" ]]; then
    cat <<EOF
[DRY-RUN] qsub \
  -N fire_${sample} \
  -q ${PBS_QUEUE} \
  ${PBS_ACCOUNT:+-A ${PBS_ACCOUNT}} \
  -l select=1:ncpus=${PBS_NCPUS}:mem=${PBS_MEM} \
  -l walltime=${PBS_WALLTIME} \
  -o ${LOGS_DIR}/${sample}.\$PBS_JOBID.out \
  -e ${LOGS_DIR}/${sample}.\$PBS_JOBID.err \
  ${PBS_EXTRA_ARGS} \
  ${job_script}
EOF
  else
    local qsub_cmd=(qsub -N "fire_${sample}" -q "$PBS_QUEUE")
    if [[ -n "${PBS_ACCOUNT}" ]]; then
      qsub_cmd+=(-A "$PBS_ACCOUNT")
    fi
    qsub_cmd+=(
      -l "select=1:ncpus=${PBS_NCPUS}:mem=${PBS_MEM}"
      -l "walltime=${PBS_WALLTIME}"
      -o "${LOGS_DIR}/${sample}.\$PBS_JOBID.out"
      -e "${LOGS_DIR}/${sample}.\$PBS_JOBID.err"
    )

    if [[ -n "${PBS_EXTRA_ARGS}" ]]; then
      # shellcheck disable=SC2206
      extra_args=( ${PBS_EXTRA_ARGS} )
      qsub_cmd+=("${extra_args[@]}")
    fi

    qsub_cmd+=("$job_script")
    "${qsub_cmd[@]}"
  fi
}

submit_sample_reduction_stage() {
  [[ -f "$CONSENSUS_BED" ]] || die "Consensus BED not found: $CONSENSUS_BED. Run consensus stage first."

  require_cmd bedtools
  validate_runner_settings
  mkdir -p "$LOGS_DIR" "$SAMPLES_RECALC_ACTUATION_DIR"

  log "Launching per-sample pileup recalculation via runner: $RUNNER"

  if [[ "$RUNNER" == "local" ]]; then
    local max_local_jobs
    max_local_jobs="${LOCAL_JOBS:-$JOBS}"

    while IFS=$'\t' read -r sample peaks pileup _rest; do
      [[ "$sample" == "sample" ]] && continue
      local out
      out="$SAMPLES_RECALC_ACTUATION_DIR/${sample}.actuation.tsv"

      if [[ -f "$out" ]]; then
        log "Skipping existing output: $out"
        continue
      fi

      submit_local_job "$pileup" "$sample" "$out" &
      while [[ "$(jobs -r | wc -l)" -ge "$max_local_jobs" ]]; do
        wait -n
      done
    done < "$MANIFEST"

    wait
    return
  fi

  while IFS=$'\t' read -r sample peaks pileup _rest; do
    [[ "$sample" == "sample" ]] && continue
    local out
    out="$SAMPLES_RECALC_ACTUATION_DIR/${sample}.actuation.tsv"

    if [[ -f "$out" ]]; then
      log "Skipping existing output: $out"
      continue
    fi

    case "$RUNNER" in
      slurm)
        submit_slurm_job "$pileup" "$sample" "$out"
        ;;
      pbs)
        submit_pbs_job "$pileup" "$sample" "$out"
        ;;
      *)
        die "Unsupported runner: $RUNNER"
        ;;
    esac
  done < "$MANIFEST"
}

cleanup() {
  if [[ "${KEEP_TEMP}" != "1" ]]; then
    rm -rf "$REDUCED_DIR"
  fi
}

MANIFEST=""
OUTDIR="fire_consensus_out"
FT_PATH=""
RUNNER="local"
SCHEDULER_CONFIG="./scheduler.conf"
STAGE="all"
JOBS="8"
DRY_RUN="0"
KEEP_TEMP="0"
CLI_ACCOUNT=""
CLI_PARTITION=""
CLI_QUEUE=""
CLI_CPUS=""
CLI_MEM=""
CLI_TIME=""
LOGS_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -m|--manifest)
      MANIFEST="$2"; shift 2 ;;
    -o|--outdir)
      OUTDIR="$2"; shift 2 ;;
    -f|--ft)
      FT_PATH="$2"; shift 2 ;;
    --runner)
      RUNNER="$2"; shift 2 ;;
    -s|--scheduler-config)
      SCHEDULER_CONFIG="$2"; shift 2 ;;
    --stage)
      STAGE="$2"; shift 2 ;;
    -j|--jobs)
      JOBS="$2"; shift 2 ;;
    --keep-temp)
      KEEP_TEMP="1"; shift ;;
    --logs-dir)
      LOGS_DIR="$2"; shift 2 ;;
    --account)
      CLI_ACCOUNT="$2"; shift 2 ;;
    --partition)
      CLI_PARTITION="$2"; shift 2 ;;
    --queue)
      CLI_QUEUE="$2"; shift 2 ;;
    --cpus)
      CLI_CPUS="$2"; shift 2 ;;
    --mem)
      CLI_MEM="$2"; shift 2 ;;
    --time)
      CLI_TIME="$2"; shift 2 ;;
    --dry-run)
      DRY_RUN="1"; shift ;;
    -h|--help)
      show_help; exit 0 ;;
    *)
      die "Unknown option: $1" ;;
  esac
done

[[ -n "$MANIFEST" ]] || { show_help; die "--manifest is required"; }
[[ -f "$MANIFEST" ]] || die "Manifest not found: $MANIFEST"

case "$RUNNER" in
  local|slurm|pbs) ;;
  *) die "Invalid --runner: $RUNNER" ;;
esac

case "$STAGE" in
  all|reduce-peaks|consensus|submit-sample-reduction) ;;
  *) die "Invalid --stage: $STAGE" ;;
esac

validate_manifest "$MANIFEST"

OUTDIR=$(abs_path "$OUTDIR")
mkdir -p "$OUTDIR"

[[ -n "$LOGS_DIR" ]] || LOGS_DIR="$OUTDIR/logs"

REDUCED_DIR="$OUTDIR/tmp_consensus_inputs"
SAMPLES_RECALC_ACTUATION_DIR="$OUTDIR/samples_recalc_actuation"
MERGED_BED_GZ="$OUTDIR/merged.4col.bed.gz"
MOCK_BAM="$OUTDIR/output.mock_bam"
OUTPUT_PEAKS="$OUTDIR/output.peaks"
CONSENSUS_BED="$OUTDIR/consensus.intervals.bed"
CONSENSUS_PEAK_IDS="$OUTDIR/consensus_peak_ids.tsv"
SAMPLE2CONS_SCRIPT="$OUTDIR/sample2consensus.sh"

mkdir -p "$REDUCED_DIR" "$SAMPLES_RECALC_ACTUATION_DIR" "$LOGS_DIR"
write_sample2consensus_script "$SAMPLE2CONS_SCRIPT"

trap cleanup EXIT

if [[ "$STAGE" == "all" || "$STAGE" == "reduce-peaks" ]]; then
  reduce_peaks_stage
fi

if [[ "$STAGE" == "all" || "$STAGE" == "consensus" ]]; then
  [[ -n "$FT_PATH" ]] || die "--ft is required for consensus stage"
  consensus_stage
fi

if [[ "$STAGE" == "all" || "$STAGE" == "submit-sample-reduction" ]]; then
  submit_sample_reduction_stage
fi

log "Done."