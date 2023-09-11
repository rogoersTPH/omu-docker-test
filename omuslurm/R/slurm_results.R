.datatable.aware <- TRUE

## TODO Expose the pre, cmd and post options

##' @title Run preparations for SLURM submission
##' @param expDir Directory of experiment
##' @param dbName Name of the database to create
##' @param dbDir Directory of the database file. Defaults to the root directory.
##' @param resultsName Name of the database table to add the results to.
##' @param resultsCols Vector of column names.
##' @param indexOn Define which index to create. Needs to be a lis of the form
##'   list(c(TABLE, COLUMN), c(TABLE, COLUMN), ...).
##' @param ncoresDT Number of data.table threads to use on each parallel
##'   cluster.
##' @param strategy Defines how to process the files. "batch" means that all
##'   files are read into a single data frame first, then the aggregation
##'   funciton is applied to that data frame and the result is added to the
##'   database. "serial" means that each individual file is processed with the
##'   aggregation function and added to the database.
##' @param appendResults If TRUE, do not add metadata to the database and only
##'   write results.
##' @param aggrFun A function for aggregating the output of readFun. First
##'   argument needs to be the output data frame of readFun and it needs to
##'   generate a data frame. The data frame should NOT contain an experiment_id
##'   column as this is added automatically. The column names needs to match the
##'   ones defined in resultsCols.
##' @param aggrFunArgs Arguments for aggrFun as a (named) list.
##' @param splitBy Split total number of scenarios into groups. Either a numeric
##'   value or a column name from your scenarios.
##' @param ntasks Number of tasks per CPU
##' @param nCPU Number of cores to reserve
##' @param mem Total amount of RAM
##' @param time Maximum time
##' @param qos Quality of service
##' @param verbose Toggle verbose output
##' @export
slurmPrepareResults <- function(expDir, dbName, dbDir = NULL,
                                resultsName = "results", resultsCols = c(
                                  "scenario_id", "date_aggregation", "date",
                                  "age_group", "incidenceRate",
                                  "prevalenceRate"
                                ),
                                indexOn = list(c("results", "scenario_id")),
                                ncoresDT = 1, strategy = "batch",
                                appendResults = FALSE,
                                aggrFun = CalcEpiOutputs,
                                aggrFunArgs = list(
                                  aggregateByAgeGroup = c("0-5", "2-10", "0-100"),
                                  aggregateByDate = "year"
                                ),
                                splitBy = 10000,
                                verbose = FALSE,
                                ntasks = 1, mem = "32G", nCPU = 1,
                                time = "06:00:00", qos = "6hours") {
  ## Get path if not given
  if (is.null(dbDir)) {
    dbDir <- openMalariaUtilities::getCache("rootDir")
  }

  ## Remove database
  unlink(file.path(dbDir, paste0(dbName, ".sqlite")))
  unlink(file.path(dbDir, paste0(dbName, ".sqlite-shm")))
  unlink(file.path(dbDir, paste0(dbName, ".sqlite-wal")))

  ## TODO chohorts and co
  coltbl <- data.table::data.table(
    names = c(
      "scenario_id", "date_aggregation", "date",
      "age_group",
      ## calculated indicators
      "prevalenceRate", "incidenceRate", "incidenceRatePerThousand",
      "tUncomp", "tSevere", "nHosp", "edeath",
      "edeathRatePerHundredThousand", "edirdeath",
      "edirdeathRatePerHundredThousand", "ddeath",
      "ddeathRatePerHundredThousand",
      ## OM measures
      openMalariaUtilities::omOutputDict()[["measure_name"]]
    ),
    types = c(
      "INTEGER", "TEXT", "TEXT", "TEXT",
      ## calculated indicators
      rep("NUMERIC", times = 12),
      ## OM measures
      rep("NUMERIC",
        times = length(
          openMalariaUtilities::omOutputDict()[["measure_name"]]
        )
      )
    )
  )

  ## Get column types by making a join
  resultsCols <- data.table::data.table(names = resultsCols)
  resultsCols <- coltbl[resultsCols, on = "names"]

  ## Add requested columns to aggrFunArgs and store everything in the cache
  aggrFunArgs[["indicators"]] <- resultsCols[["names"]]
  temp <- list(
    cAggrFun = aggrFun, cAggrFunArgs = aggrFunArgs
  )
  openMalariaUtilities::putCache(x = "slurm_results", value = temp)
  openMalariaUtilities::syncCache(
    path = openMalariaUtilities::getCache(x = "experimentDir")
  )

  ## Create a submission script
  filename <- file.path(
    openMalariaUtilities::getCache(x = "experimentDir"), "slurm_results.sh"
  )
  expName <- openMalariaUtilities::getCache(x = "experimentName")
  .writeSlurm(
    jobName = paste0(expName, "_results"),
    ntasks = ntasks,
    mem = mem,
    nCPU = nCPU,
    time = time,
    qos = qos,
    output = file.path(
      file.path(openMalariaUtilities::getCache(x = "logsDir"), "results"),
      paste0("slurm_", expName, "_results")
    ),
    error = file.path(
      file.path(openMalariaUtilities::getCache(x = "logsDir"), "results"),
      paste0("slurm_", expName, "_results")
    ),
    cmd = list(paste("singularity exec", file.path(openMalariaUtilities::getCache(x = "experimentDir"), "..", "omu-docker-test_main.sif"), "Rscript", file.path(
      openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_results.R"
    ), "$SLURM_CPUS_PER_TASK")),
    file = filename
  )

  ## Create R script
  if (is.character(splitBy)) {
    .writeSettingsLoop(
      expDir, dbName, dbDir, as.list(resultsCols), indexOn, ncoresDT,
      verbose, strategy, splitBy
    )
  } else if (is.numeric(splitBy)) {
    .write10kLoop(
      expDir, dbName, dbDir, resultsName, as.list(resultsCols), indexOn,
      ncoresDT, verbose, strategy, splitBy
    )
  } else {
    stop("splitBy needs be a number or a column name.")
  }
}

.write10kLoop <- function(expDir, dbName, dbDir, resultsName, resultsCols,
                          indexOn, ncoresDT, verbose, strategy, num) {
  cat(
    "#!/usr/bin/env Rscript

## Get arguments
args <- commandArgs(trailingOnly = TRUE)
ncores <- as.numeric(args[1])

## Set correct working directory\n",
    "setwd(dir = \"", paste0(openMalariaUtilities::getCache(x = "rootDir")), "\")

## Load library
library(openMalariaUtilities)

## Load cached data
loadExperiment(\"", paste0(openMalariaUtilities::getCache(x = "experimentDir")), "\")

## Read scenarios
scens <- readScenarios()
batches <- splitSeq(1:nrow(scens), n = ", noquote(deparse(num)), ")

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
expDir = ", noquote(deparse(expDir)), ",
dbName = ", noquote(deparse(dbName)), ",
dbDir = ", noquote(deparse(dbDir)), ",
replace = FALSE,
fileFun = fileCol, fileFunArgs = list(scens = scens, ffilter = batches[[batch]]),
aggrFun = funs$cAggrFun, aggrFunArgs = funs$cAggrFunArgs,
ncores = ncores, ncoresDT = ", noquote(deparse(ncoresDT)), ",
strategy = ", noquote(deparse(strategy)), ",
resultsName = ", noquote(deparse(resultsName)), ",
resultsCols = ", noquote(deparse(resultsCols)), ",
indexOn = ", noquote(deparse(indexOn)), ",
verbose = ", noquote(deparse(verbose)), "
  )

gc(verbose = TRUE)
}
",
    file = file.path(
      openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_results.R"
    ),
    sep = ""
  )
}

.writeSettingsLoop <- function(expDir, dbName, dbDir, resultsCols, indexOn,
                               ncoresDT, verbose, strategy, colName) {
  cat(
    "#!/usr/bin/env Rscript

## Get arguments
args <- commandArgs(trailingOnly = TRUE)
ncores <- as.numeric(args[1])

## Set correct working directory\n",
    "setwd(dir = \"", paste0(openMalariaUtilities::getCache(x = "rootDir")), "\")

## Load library
library(openMalariaUtilities)

## Load cached data
loadExperiment(\"", paste0(openMalariaUtilities::getCache(x = "experimentDir")), "\")

## Read scenarios
scens <- readScenarios()

funs <- getCache(x = \"slurm_results\")

fileCol <- function(scens, cname) {
  filesAggr <-
    scens[eval(parse(text = paste0(\"", as.symbol(colName), " == '\", cname, \"'\"))), file]
  return(filesAggr)
}

for (s in unique(scens[[", noquote(deparse(colName)), "]]) {
message(paste0(\"Working on \", s))

collectResults(
expDir = ", noquote(deparse(expDir)), ",
dbName = ", noquote(deparse(dbName)), ",
dbDir = ", noquote(deparse(dbDir)), ",
replace = FALSE,
fileFun = fileCol, fileFunArgs = list(scens = scens, cname = s),
aggrFun = funs$cAggrFun, aggrFunArgs = funs$cAggrFunArgs,
ncores = ncores, ncoresDT = ", noquote(deparse(ncoresDT)), ",
strategy = ", noquote(deparse(strategy)), ",
resultsName = paste0(\"results_\", s),
resultsCols = ", noquote(deparse(resultsCols)), ",
indexOn = ", noquote(deparse(indexOn)), ",
verbose = ", noquote(deparse(verbose)), "
  )

gc(verbose = TRUE)
}
",
    file = file.path(
      openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_results.R"
    ),
    sep = ""
  )
}

##' @title Submit results job to SLURM
##' @param moreArgs Further arguments for SLURM's sbatch command, e.g.
##'   c("--arg1", "--arg2")
##' @param ... Deprecated arguments
##' @export
slurmRunResults <- function(moreArgs = NULL, ...) {
  system(
    command = paste0(
      "sbatch --wait ", if (!is.null(moreArgs)) {
        paste0(moreArgs, " ", collapse = " ")
      }, file.path(
        openMalariaUtilities::getCache("experimentDir"),
        "slurm_results.sh"
      )
    )
  )
}
