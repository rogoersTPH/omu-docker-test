## Setup
dir <- file.path(tempdir(), "test-slurm_scenarios")
openMalariaUtilities::clearCache()
openMalariaUtilities::setupDirs(experimentName = "test", rootDir = dir, replace = TRUE)

test_that("slurmPrepareScenarios works", {
  scenarios <- data.frame(
    futITNcov = c(.65),
    futIRScov = c(0, .8),
    EIR = c(5, 25),
    setting = c("alpha"),
    pop = c(1:10),
    seed = 1,
    setting = c(1:450)
  )

  ## Bash submission script created with correct content
  slurmPrepareScenarios(expName = "test", scenarios = scenarios)

  expected <- paste(capture.output(cat("#!/bin/bash
#SBATCH --job-name=test_scenarios
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=1G
#SBATCH --output=", openMalariaUtilities::getCache(x = "experimentDir"), "/logs/scenarios/slurm_test_scenarios_%A_%a.log
#SBATCH --error=", openMalariaUtilities::getCache(x = "experimentDir"), "/logs/scenarios/slurm_test_scenarios_%A_%a_error.log
#SBATCH --array=1-3
#SBATCH --time=00:10:00
#SBATCH --qos=30min
ID=$(expr ${SLURM_ARRAY_TASK_ID} - 0)

module purge
module load R/4.1.2-foss-2018b-Python-3.6.6

Rscript ", openMalariaUtilities::getCache(x = "experimentDir"), "/slurm_run_scenarios.R $ID $SLURM_CPUS_PER_TASK

", sep = "")), sep = "", collapse = "\n")

  actual <- paste(
    capture.output(
      cat(
        readLines(file.path(openMalariaUtilities::getCache(x = "experimentDir"), "slurm_scenarios.sh")),
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

## Set correct working directory
setwd(dir = \"", dir, "\")

## Load library
library(openMalariaUtilities)

## Load cached data
loadExperiment(\"", openMalariaUtilities::getCache(x = "experimentDir"), "\")

## Get range of scenarios to create
slurm <- getCache(x = \"slurm_scenarios\")
ID <- args[1]
ncores <- as.numeric(args[2])
rowStart <- slurm$scen_batches[[ID]][1]
rowEnd <- slurm$scen_batches[[ID]][length(slurm$scen_batches[[ID]])]

## Read scenarios
scens <- readScenarios()

baseFile <- getCache(x = \"baseXml\")
prefix <- getCache(x = \"experimentName\")

setupScenarios(
  scenarios = scens, baseFile = baseFile, rowStart = rowStart, rowEnd = rowEnd,
  prefix = prefix, ncores = ncores
)
",
    sep = ""
  )), sep = "", collapse = "\n")

  actual <- paste(
    capture.output(
      cat(
        readLines(
          file.path(openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_scenarios.R")
        ),
        sep = "\n"
      )
    ),
    sep = "", collapse = "\n"
  )

  expect_equal(actual, expected)
})
