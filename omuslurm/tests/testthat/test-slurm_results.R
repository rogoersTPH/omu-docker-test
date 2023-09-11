## Setup
dir <- file.path(tempdir(), "test-slurm_results")
openMalariaUtilities::clearCache()
openMalariaUtilities::setupDirs(experimentName = "test", rootDir = dir, replace = TRUE)

CalcEpiOutputs <- function(x) {
  return(x)
}

test_that("slurmPrepareResults works", {
  ## Split by settings
  slurmPrepareResults(
    expDir = dir, dbName = "test",
    resultsCols = c(
      "scenario_id", "date_aggregation", "date", "age_group", "prevalenceRate",
      "nUncomp", "nHost", "nSevere", "incidenceRate", "nTreatments1",
      "nTreatments2", "nTreatments3"),
    splitBy = "setting", aggrFun = CalcEpiOutputs,
    aggrFunArgs = list(
      aggregateByAgeGroup = c("0-5", "2-10", "0-100"),
      aggregateByDate = "year"
    )
  )

  expected <- paste(capture.output(cat("#!/bin/bash
#SBATCH --job-name=test_results
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=32G
#SBATCH --output=", openMalariaUtilities::getCache(x = "experimentDir"), "/logs/results/slurm_test_results_%A_%a.log
#SBATCH --error=", openMalariaUtilities::getCache(x = "experimentDir"), "/logs/results/slurm_test_results_%A_%a_error.log
#SBATCH --time=06:00:00
#SBATCH --qos=6hours

module purge
module load R/4.1.2-foss-2018b-Python-3.6.6

Rscript ", openMalariaUtilities::getCache(x = "experimentDir"), "/slurm_run_results.R $SLURM_CPUS_PER_TASK

", sep = "")), sep = "", collapse = "\n")

  actual <- paste(
    capture.output(
      cat(
        readLines(file.path(openMalariaUtilities::getCache(x = "experimentDir"), "slurm_results.sh")),
        sep = "\n"
      )
    ),
    sep = "", collapse = "\n"
  )

  expect_equal(actual, expected)

  ## Rscript created with correct content
  expected <- paste(capture.output(cat(
    "#!/usr/bin/env Rscript

## Get arguments
args <- commandArgs(trailingOnly = TRUE)
ncores <- as.numeric(args[1])

## Set correct working directory
setwd(dir = \"", dir, "\")

## Load library
library(openMalariaUtilities)

## Load cached data
loadExperiment(\"", openMalariaUtilities::getCache(x = "experimentDir"), "\")

## Read scenarios
scens <- readScenarios()

funs <- getCache(x = \"slurm_results\")

fileCol <- function(scens, cname) {
  filesAggr <-
    scens[eval(parse(text = paste0(\"setting == '\", cname, \"'\"))), file]
  return(filesAggr)
}

for (s in unique(scens[[\"setting\"]]) {
message(paste0(\"Working on \", s))

collectResults(
expDir = \"", dir, "\",
dbName = \"test\",
dbDir = \"", dir, "\",
replace = FALSE,
fileFun = fileCol, fileFunArgs = list(scens = scens, cname = s),
aggrFun = funs$cAggrFun, aggrFunArgs = funs$cAggrFunArgs,
ncores = ncores, ncoresDT = 1,
strategy = \"batch\",
resultsName = paste0(\"results_\", s),
resultsCols = list(names = c(\"scenario_id\", \"date_aggregation\", \"date\", \"age_group\", \"prevalenceRate\", \"nUncomp\", \"nHost\", \"nSevere\", \"incidenceRate\", \"nTreatments1\", \"nTreatments2\", \"nTreatments3\"), types = c(\"INTEGER\", \"TEXT\", \"TEXT\", \"TEXT\", \"NUMERIC\", \"NUMERIC\", \"NUMERIC\", \"NUMERIC\", \"NUMERIC\", \"NUMERIC\", \"NUMERIC\", \"NUMERIC\")),
indexOn = list(c(\"results\", \"scenario_id\")),
verbose = FALSE
  )

gc(verbose = TRUE)
}
",
    sep = ""
  )), sep = "", collapse = "\n")

  actual <- paste(
    capture.output(
      cat(
        readLines(
          file.path(openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_results.R")
        ),
        sep = "\n"
      )
    ),
    sep = "", collapse = "\n"
  )

  expect_equal(actual, expected)

  ## 10k loop
  slurmPrepareResults(
    expDir = dir, dbName = "test",
    aggrFun = CalcEpiOutputs,
    aggrFunArgs = list(
      aggregateByAgeGroup = c("0-5", "2-10", "0-100"),
      aggregateByDate = "year"
    )
  )

  expected <- paste(capture.output(cat("#!/bin/bash
#SBATCH --job-name=test_results
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=32G
#SBATCH --output=", openMalariaUtilities::getCache(x = "experimentDir"), "/logs/results/slurm_test_results_%A_%a.log
#SBATCH --error=", openMalariaUtilities::getCache(x = "experimentDir"), "/logs/results/slurm_test_results_%A_%a_error.log
#SBATCH --time=06:00:00
#SBATCH --qos=6hours

module purge
module load R/4.1.2-foss-2018b-Python-3.6.6

Rscript ", openMalariaUtilities::getCache(x = "experimentDir"), "/slurm_run_results.R $SLURM_CPUS_PER_TASK

", sep = "")), sep = "", collapse = "\n")

  actual <- paste(
    capture.output(
      cat(
        readLines(file.path(openMalariaUtilities::getCache(x = "experimentDir"), "slurm_results.sh")),
        sep = "\n"
      )
    ),
    sep = "", collapse = "\n"
  )

  expect_equal(actual, expected)

  ## Rscript created with correct content
  expected <- paste(capture.output(cat(
    "#!/usr/bin/env Rscript

## Get arguments
args <- commandArgs(trailingOnly = TRUE)
ncores <- as.numeric(args[1])

## Set correct working directory
setwd(dir = \"", dir, "\")

## Load library
library(openMalariaUtilities)

## Load cached data
loadExperiment(\"", openMalariaUtilities::getCache(x = "experimentDir"), "\")

## Read scenarios
scens <- readScenarios()
batches <- splitSeq(1:nrow(scens), n = 10000)

funs <- getCache(x = \"slurm_results\")

fileCol <- function(scens, ffilter) {
  scens <- data.table::as.data.table(scens)
  ffilter <- substitute(ffilter)
  fToUse <- scens[eval(ffilter), file]
  return(fToUse)
}

for (batch in names(batches)) {
lower_scen <- min(batches[[batch]])
upper_scen <- max(batches[[batch]])

message(paste0(\"Working on scenario \", lower_scen, \" to \", upper_scen))

collectResults(
expDir = \"", dir, "\",
dbName = \"test\",
dbDir = \"", dir, "\",
replace = FALSE,
fileFun = fileCol, fileFunArgs = list(scens = scens, ffilter = batches[[batch]]),
aggrFun = funs$cAggrFun, aggrFunArgs = funs$cAggrFunArgs,
ncores = ncores, ncoresDT = 1,
strategy = \"batch\",
resultsName = \"results\",
resultsCols = list(names = c(\"scenario_id\", \"date_aggregation\", \"date\", \"age_group\", \"incidenceRate\", \"prevalenceRate\"), types = c(\"INTEGER\", \"TEXT\", \"TEXT\", \"TEXT\", \"NUMERIC\", \"NUMERIC\")),
indexOn = list(c(\"results\", \"scenario_id\")),
verbose = FALSE
  )

gc(verbose = TRUE)
}
",
    sep = ""
  )), sep = "", collapse = "\n")

  actual <- paste(
    capture.output(
      cat(
        readLines(
          file.path(openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_results.R")
        ),
        sep = "\n"
      )
    ),
    sep = "", collapse = "\n"
  )

  expect_equal(actual, expected)
})
