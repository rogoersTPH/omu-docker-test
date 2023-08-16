## Run OpenMalaria simulations via SLURM
## The general workflow is:
##  - Get the number of scenarios
##  - Create SLURM launch script and RScript submitting each scenarios to SLURM


## TODO Expose the pre, cmd and post options

##' @title Run preparations for SLURM submission
##' @param expName Name of experiment
##' @param scenarios Scenario data frame
##' @param bSize Number of parrellel processed files
##' @param ntasks Number of tasks per CPU
##' @param nCPU Number of cores to reserve
##' @param memCPU Memory per CPU
##' @param time Maximum time
##' @param qos Quality of service
##' @param verbose If TRUE, use OpenMalaria's verbose output.
##' @export
slurmPrepareSimulations <- function(expName, scenarios, bSize = 200,
                                    ntasks = 1, memCPU = "1G", nCPU = 1,
                                    time = "06:00:00", qos = "6hours",
                                    verbose = FALSE) {
  ## Calculate number of scenarios and divide them into packages of 200
  nscens <- as.integer(row.names(scenarios))
  batches <- openMalariaUtilities::splitSeq(nscens, bSize)
  nbatches <- length(batches)

  ## Store into cache
  temp <- list(
    expName = expName,
    scen_batches = batches,
    scen_nbatches = nbatches
  )
  openMalariaUtilities::putCache(x = "slurm_simulation", value = temp)
  openMalariaUtilities::syncCache(
    path = openMalariaUtilities::getCache(x = "experimentDir")
  )

  ## Create a submission script
  filename <- file.path(
    openMalariaUtilities::getCache(x = "experimentDir"), "slurm_simulation.sh"
  )
  .writeSlurm(
    jobName = paste0(expName, "_simulation"),
    ntasks = ntasks,
    memCPU = memCPU,
    nCPU = nCPU,
    array = nbatches,
    time = time,
    qos = qos,
    output = file.path(
      file.path(openMalariaUtilities::getCache(x = "logsDir"), "simulation"),
      paste0("slurm_", expName, "_simulation")
    ),
    error = file.path(
      file.path(openMalariaUtilities::getCache(x = "logsDir"), "simulation"),
      paste0("slurm_", expName, "_simulation")
    ),
    pre = list(
      ## REVIEW It seems that the OpenMalaria and R modules are conflicting.
      ##        Maybe we can find a way around this without forcing autoswap.
      "export LMOD_DISABLE_SAME_NAME_AUTOSWAP=\"no\"",
      "module purge",
      "module load R/4.1.2-foss-2018b-Python-3.6.6",
      "module load OpenMalaria/44.0-iomkl-2019.01",
      ## This is quiet important, otherwise OpenMalaria cannot find the
      ## supporting files (*.xsd, etc)
      paste0("cd ", openMalariaUtilities::getCache(x = "experimentDir"))
    ),
    cmd = list(paste("Rscript", file.path(
      openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_simulation.R"
    ), "$ID", "$SLURM_CPUS_PER_TASK")),
    file = filename
  )

  ## Create R script
  cat(
    "#!/usr/bin/env Rscript

## Get arguments
args <- commandArgs(trailingOnly = TRUE)

## Set correct working directory\n",
    "setwd(dir = \"", paste0(openMalariaUtilities::getCache(x = "experimentDir")), "\")

## Verbose output
verbose <- ", ifelse(verbose == TRUE, paste0("TRUE"), paste0("FALSE")), "

## Load library
library(openMalariaUtilities)

## Load cached data
loadExperiment(\"", paste0(openMalariaUtilities::getCache(x = "experimentDir")), "\")

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
    file = file.path(
      openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_simulation.R"
    ),
    sep = ""
  )
}


##' @title Submit simulation job to SLURM
##' @param moreArgs Further arguments for SLURM's sbatch command, e.g.
##'   c("--arg1", "--arg2")
##' @param n Current try.
##' @param limit Maximum number of tries/resubmissions.
##' @param ... Deprecated arguments
##' @export
slurmRunSimulation <- function(moreArgs = NULL, n = 0, limit = 3, ...) {
  if (n < limit) {
    message("(Re)Submitting jobs... ")
    n <- n + 1
    jobID <- system(command = paste0(
      "sbatch --wait ", if (!is.null(moreArgs)) {
        paste0(moreArgs, " ", collapse = " ")
      }, file.path(
        openMalariaUtilities::getCache("experimentDir"),
        "slurm_simulation.sh"
      )
    ), intern = TRUE)

    jobID <- as.numeric(
      gsub(
        pattern = "Submitted batch job ([0-9]+)", replacement = "\\1", x = jobID
      )
    )

    status <- utils::read.table(
      text = system(
        paste0(
          "sacct -j ", jobID,
          " --format JobID,State --noheader --parsable2 --noconvert"
        ),
        intern = TRUE, ignore.stderr = TRUE
      ), header = FALSE, sep = "|"
    )

    ## Filter out .batch/.extern substrings
    status <- status[!grepl(pattern = "[0-9]+_[0-9]+\\..+", x = status$V1), ]
    ## Keep only failed jobs
    fails <- status[status$V2 %in% c("BOOT_FAIL", "NODE_FAIL", "FAILED"), ]
    ## Get the batch numbers
    fails$V3 <- as.numeric(gsub(
      pattern = "[0-9]+_([0-9]+)", replacement = "\\1", x = fails$V1
    ))
    new_runs <- fails$V3
    ## Inform about non-failed or completed jobs
    others <- status[!status$V2 %in% c("BOOT_FAIL", "NODE_FAIL", "FAILED", "COMPLETED", "PENDING", "RUNNING"), ]
    if (nrow(others) > 0) {
      message("Please check the state of the following jobs:\n")
      print(data.frame(JobID = others$V1, State = others$V2))
      message("")
    }
    if (length(new_runs > 0)) {
      message(
        paste0(
          "Trying to run the following jobs again: ",
          paste0(fails$V1, collapse = ", ")
        )
      )
      message(paste0(limit - n, " tries left"))
      ## Prepare new runs
      old_f <- readLines(file.path(
        openMalariaUtilities::getCache("experimentDir"),
        "slurm_simulation.sh"
      ))
      new_f <- gsub(
        pattern = "#SBATCH --array=.*",
        replacement = paste0("#SBATCH --array=", paste0(new_runs, collapse = ",")),
        x = old_f
      )
      writeLines(new_f, con = file.path(
        openMalariaUtilities::getCache("experimentDir"),
        "slurm_simulation.sh"
      ))
      slurmRunSimulation(moreArgs = moreArgs, n = n, limit = limit)
    } else {
      message("All jobs submitted!")
      return(TRUE)
    }
  } else {
    stop("Some jobs have failed!")
  }
}
