## Create scenarios from a base xml file with SLURM.
## The general workflow is:
##  - Get the number of scenarios and divide into equally sized batches
##  - Store this information and scenarios on disk
##  - Create SLURM launch script
##  - Create RScript doing the work


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
##' @export
slurmPrepareScenarios <- function(expName, scenarios, bSize = 200,
                                  ntasks = 1, memCPU = "1G", nCPU = 1,
                                  time = "00:10:00", qos = "30min") {
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
  openMalariaUtilities::putCache(x = "slurm_scenarios", value = temp)
  openMalariaUtilities::syncCache(
    path = openMalariaUtilities::getCache(x = "experimentDir")
  )

  ## Create a scenario job
  filename <- file.path(
    openMalariaUtilities::getCache(x = "experimentDir"), "slurm_scenarios.sh"
  )
  .writeSlurm(
    jobName = paste0(expName, "_scenarios"),
    ntasks = ntasks,
    memCPU = memCPU,
    nCPU = nCPU,
    array = nbatches,
    time = time,
    qos = qos,
    output = file.path(
      file.path(openMalariaUtilities::getCache(x = "logsDir"), "scenarios"),
      paste0("slurm_", expName, "_scenarios")
    ),
    error = file.path(
      file.path(openMalariaUtilities::getCache(x = "logsDir"), "scenarios"),
      paste0("slurm_", expName, "_scenarios")
    ),
    cmd = list(paste("singularity exec", file.path(openMalariaUtilities::getCache(x = "experimentDir"), "..", "omu-docker-test_main.sif"), "Rscript", file.path(
      openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_scenarios.R"
    ), "$ID", "$SLURM_CPUS_PER_TASK")),
    file = filename
  )

  ## Create R script
  cat(
    "#!/usr/bin/env Rscript

## Get arguments
args <- commandArgs(trailingOnly = TRUE)

## Set correct working directory\n",
    "setwd(dir = \"", paste0(openMalariaUtilities::getCache(x = "rootDir")), "\")

## Load library
library(openMalariaUtilities)

## Load cached data
loadExperiment(\"", paste0(openMalariaUtilities::getCache(x = "experimentDir")), "\")

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
    file = file.path(
      openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_scenarios.R"
    ),
    sep = ""
  )
}


##' @title Submit scenario creation job to SLURM
##' @param moreArgs Further arguments for SLURM's sbatch command, e.g.
##'   c("--arg1", "--arg2")
##' @param n Current try.
##' @param limit Maximum number of tries/resubmissions.
##' @param ... Deprecated arguments
##' @export
slurmCreateScenarios <- function(moreArgs = NULL, n = 0, limit = 3, ...) {
  if (n < limit) {
    message("(Re)Submitting jobs... ")
    n <- n + 1
    jobID <- system(command = paste0(
      "sbatch --wait ", if (!is.null(moreArgs)) {
        paste0(moreArgs, " ", collapse = " ")
      }, file.path(
        openMalariaUtilities::getCache("experimentDir"),
        "slurm_scenarios.sh"
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
        "slurm_scenarios.sh"
      ))
      new_f <- gsub(
        pattern = "#SBATCH --array=.*",
        replacement = paste0("#SBATCH --array=", paste0(new_runs, collapse = ",")),
        x = old_f
      )
      writeLines(new_f, con = file.path(
        openMalariaUtilities::getCache("experimentDir"),
        "slurm_scenarios.sh"
      ))
      slurmCreateScenarios(moreArgs = moreArgs, n = n, limit = limit)
    } else {
      message("All jobs submitted!")
      return(TRUE)
    }
  } else {
    stop("Some jobs have failed!")
  }
}
