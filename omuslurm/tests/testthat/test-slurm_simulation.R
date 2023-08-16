## Setup
dir <- file.path(tempdir(), "test-slurm_simulation")
openMalariaUtilities::clearCache()
openMalariaUtilities::setupDirs(experimentName = "test", rootDir = dir, replace = TRUE)

test_that("slurmPrepareSimulations works", {
  scenarios <- data.frame(setting = c(1:450))

  ## Bash submission script created with correct content
  slurmPrepareSimulations(expName = "test", scenarios = scenarios)

  expected <- paste(capture.output(cat("#!/bin/bash
#SBATCH --job-name=test_simulation
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=1G
#SBATCH --output=", openMalariaUtilities::getCache(x = "experimentDir"), "/logs/simulation/slurm_test_simulation_%A_%a.log
#SBATCH --error=", openMalariaUtilities::getCache(x = "experimentDir"), "/logs/simulation/slurm_test_simulation_%A_%a_error.log
#SBATCH --array=1-3
#SBATCH --time=06:00:00
#SBATCH --qos=6hours
ID=$(expr ${SLURM_ARRAY_TASK_ID} - 0)

export LMOD_DISABLE_SAME_NAME_AUTOSWAP=\"no\"
module purge
module load R/4.1.2-foss-2018b-Python-3.6.6
module load OpenMalaria/44.0-iomkl-2019.01
cd ", openMalariaUtilities::getCache(x = "experimentDir"), "

Rscript ", openMalariaUtilities::getCache(x = "experimentDir"), "/slurm_run_simulation.R $ID $SLURM_CPUS_PER_TASK

", sep = "")), sep = "", collapse = "\n")

  actual <- paste(
    capture.output(
      cat(
        readLines(file.path(openMalariaUtilities::getCache(x = "experimentDir"), "slurm_simulation.sh")),
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
setwd(dir = \"", openMalariaUtilities::getCache(x = "experimentDir"), "\")

## Verbose output
verbose <- FALSE

## Load library
library(openMalariaUtilities)

## Load cached data
loadExperiment(\"", openMalariaUtilities::getCache(x = "experimentDir"), "\")

## Get scenario number to run
slurm <- getCache(x = \"slurm_simulation\")
ID <- args[1]
ncores <- as.numeric(args[2])
rowStart <- slurm$scen_batches[[ID]][1]
rowEnd <- slurm$scen_batches[[ID]][length(slurm$scen_batches[[ID]])]

## Read scenarios
scens <- readScenarios()

runSimulations(
  scenarios = scens, rowStart = rowStart, rowEnd = rowEnd, verbose = verbose,
  ncores = ncores
)
",
    sep = ""
  )), sep = "", collapse = "\n")

  actual <- paste(
    capture.output(
      cat(
        readLines(
          file.path(openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_simulation.R")
        ),
        sep = "\n"
      )
    ),
    sep = "", collapse = "\n"
  )

  expect_equal(actual, expected)

  ## Verbose output on
  slurmPrepareSimulations(expName = "test", scenarios = scenarios, verbose = TRUE)

  expected <- paste(capture.output(cat(
    "#!/usr/bin/env Rscript

## Get arguments
args <- commandArgs(trailingOnly = TRUE)

## Set correct working directory
setwd(dir = \"", openMalariaUtilities::getCache(x = "experimentDir"), "\")

## Verbose output
verbose <- TRUE

## Load library
library(openMalariaUtilities)

## Load cached data
loadExperiment(\"", openMalariaUtilities::getCache(x = "experimentDir"), "\")

## Get scenario number to run
slurm <- getCache(x = \"slurm_simulation\")
ID <- args[1]
ncores <- as.numeric(args[2])
rowStart <- slurm$scen_batches[[ID]][1]
rowEnd <- slurm$scen_batches[[ID]][length(slurm$scen_batches[[ID]])]

## Read scenarios
scens <- readScenarios()

runSimulations(
  scenarios = scens, rowStart = rowStart, rowEnd = rowEnd, verbose = verbose,
  ncores = ncores
)
",
    sep = ""
  )), sep = "", collapse = "\n")

  actual <- paste(
    capture.output(
      cat(
        readLines(
          file.path(openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_simulation.R")
        ),
        sep = "\n"
      )
    ),
    sep = "", collapse = "\n"
  )

  expect_equal(actual, expected)
})
