##' @title Run preparations for SGE submission
##' @param expName Name of experiment
##' @param scenarios Scenario data frame
##' @param bSize Number of parrellel processed files
##' @param nCPU Number of cores to reserve
##' @param memCPU Memory per CPU
##' @param time Maximum time
##' @param qos Quality of service
##' @export
sgePrepareScenarios <- function(expName, scenarios, bSize = 200,
                                memCPU = "1G", nCPU = 1,
                                time = "00:10:00", qos = "all.q", ...) {
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

  cat(
    "#$ -N ", paste0(expName, "_scenarios"), "
#$ -pe smp ", nCPU, "
#$ -l h_vmem=", memCPU, "
#$ -o ", file.path(
      file.path(openMalariaUtilities::getCache(x = "logsDir"), "scenarios"),
      paste0("sge_", expName, "_scenarios_$JOB_ID.log")
), "
#$ -e ", file.path(
        file.path(openMalariaUtilities::getCache(x = "logsDir"), "scenarios"),
        paste0("sge_", expName, "_scenarios_$JOB_ID_error.log")
), "
#$ -t 1-", nbatches, "
#$ -l h_rt=", time, "
#$ -q ", qos, "
ID=$(expr ${SGE_TASK_ID} - 0)

singularity exec ", file.path(openMalariaUtilities::getCache(x = "experimentDir"), "..", "omu-docker-test_main.sif"), " Rscript ", file.path(
  openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_scenarios.R"
), " $ID $NSLOTS
"
,
file = file.path(
  openMalariaUtilities::getCache(x = "experimentDir"), "sge_scenarios.sh"
),
sep = ""
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

##' @title Run preparations for SGE submission
##' @param expName Name of experiment
##' @param scenarios Scenario data frame
##' @param bSize Number of parrellel processed files
##' @param nCPU Number of cores to reserve
##' @param memCPU Memory per CPU
##' @param time Maximum time
##' @param qos Quality of service
##' @param verbose If TRUE, use OpenMalaria's verbose output.
##' @export
sgePrepareSimulations <- function(expName, scenarios, bSize = 200,
                                  memCPU = "1G", nCPU = 1,
                                  time = "06:00:00", qos = "all.q",
                                  verbose = FALSE, ...) {
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

  cat(
    "#$ -N ", paste0(expName, "_simulation"), "
#$ -pe smp ", nCPU, "
#$ -l h_vmem=", memCPU, "
#$ -o ", file.path(
      file.path(openMalariaUtilities::getCache(x = "logsDir"), "simulation"),
      paste0("sge_", expName, "_simulation_$JOB_ID.log")
), "
#$ -e ", file.path(
        file.path(openMalariaUtilities::getCache(x = "logsDir"), "simulation"),
        paste0("sge_", expName, "_simulation_$JOB_ID_error.log")
), "
#$ -t 1-", nbatches, "
#$ -l h_rt=", time, "
#$ -q ", qos, "
ID=$(expr ${SGE_TASK_ID} - 0)

OPENBLAS_NUM_THREADS=1 singularity exec ", file.path(openMalariaUtilities::getCache(x = "experimentDir"), "..", "omu-docker-test_main.sif"), " Rscript ", file.path(
  openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_simulation.R"
), " $ID $NSLOTS
"
,
file = file.path(
  openMalariaUtilities::getCache(x = "experimentDir"), "sge_simulation.sh"
),
sep = ""
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

##' @title Run preparations for SGE submission
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
##' @param nCPU Number of cores to reserve
##' @param mem Total amount of RAM
##' @param time Maximum time
##' @param qos Quality of service
##' @param verbose Toggle verbose output
##' @export
sgePrepareResults <- function(expDir, dbName, dbDir = NULL,
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
                                mem = "32G", nCPU = 1,
                                time = "06:00:00", qos = "all.q", ...) {
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
  expName <- openMalariaUtilities::getCache(x = "experimentName")

  cat(
    "#$ -N ", paste0(expName, "_results"), "
#$ -pe smp ", nCPU, "
#$ -l h_vmem=", mem, "
#$ -o ", file.path(
      file.path(openMalariaUtilities::getCache(x = "logsDir"), "results"),
      paste0("sge_", expName, "_results_$JOB_ID.log")
), "
#$ -e ", file.path(
        file.path(openMalariaUtilities::getCache(x = "logsDir"), "results"),
        paste0("sge_", expName, "_results_$JOB_ID_error.log")
), "
#$ -l h_rt=", time, "
#$ -q ", qos, "
ID=$(expr ${SGE_TASK_ID} - 0)

OPENBLAS_NUM_THREADS=1 singularity exec ", file.path(openMalariaUtilities::getCache(x = "experimentDir"), "..", "omu-docker-test_main.sif"), " Rscript ", file.path(
  openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_results.R"
), " $ID $NSLOTS
"
,
file = file.path(
  openMalariaUtilities::getCache(x = "experimentDir"), "sge_results.sh"
),
sep = ""
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

##' @title Create script for SGE submission
##' @export
sgePrepareRun <- function() {

  cat(
    "#!/usr/bin/env Rscript

check_scenarios_created <- function(experiment_folder, stop_if_missing = FALSE) {
  # Identify the scenarios folder and data frame
  scenario_folder <- file.path(experiment_folder, \"scenarios\")
  scenario_rds_file <- file.path(experiment_folder, \"cache\", \"scenarios.rds\")

  # Check if the scenario folder and data frame are not empty
  if (!file.exists(scenario_folder)) {
    stop(\"The folder with scenarios does not exist!\")
  }
  if (!file.exists(scenario_rds_file)) {
    stop(\"Scenario table (scenarios.rds) does not exist!\")
  }

  # Extract the generated scenarios
  scenario_list_created <- list.files(path = scenario_folder, pattern = \"xml\")
  created_scenarios <- length(scenario_list_created)
  # Extract the full set of defined scenarios that should have been generated
  scenario_list_required <- readRDS(scenario_rds_file)
  required_scenarios <- nrow(scenario_list_required)
  if (!(required_scenarios > 0)) {
    stop(\"No scenarios stored for the experiment, scenarios.rds is empty!\")
  }
  # Create a file to store the file names of the missing scenarios
  missing_file <- file.path(experiment_folder, \"missing_scenarios.txt\")

  if (created_scenarios == required_scenarios) {
    print(\"All scenarios were successfully created.\")

    # If the missing file was previously created, remove it
    if (file.exists(missing_file)) {
      file.remove(missing_file)
    }
  } else {
    # Retrieve the missing scenarios and save their file names to a file
    ind_missing <- which(
      !(scenario_list_required$file %in% scenario_list_created)
    )
    list_missing <- scenario_list_required$file[ind_missing]
    write(list_missing, missing_file)
    err_msg <- paste(
      \"Not all scenarios were generated.\",
      \"Number of missing scenarios: \",
      abs(created_scenarios - required_scenarios),
      \". Check the missing_scenarios.txt to see which scenarios\",
      \"are missing\", \"and if using the cluster, check the\",
      \"logs/scenarios/ folder for error\",
      \"messages encountered during the scenario creation.\"
    )

    if (stop_if_missing) {
      stop(err_msg)
    } else {
      print(\"WARNING:\")
      message(err_msg)
    }
  }
}

check_simulations_created <- function(experiment_folder,
                                      stop_if_missing = FALSE) {
  # Create a file to store the job IDs of the missing simulations
  missing_om_file <- file.path(experiment_folder, \"missing_om_simulations.txt\")

  # Identify the scenarios folder and data frame
  simulation_folder <- file.path(experiment_folder, \"outputs\")
  scenario_rds_file <- file.path(experiment_folder, \"cache\", \"scenarios.rds\")

  # Check if the scenario folder and data frame are not empty
  if (!file.exists(simulation_folder)) {
    stop(\"The folder with simulation outputs does not exist!\")
  }
  if (!file.exists(scenario_rds_file)) {
    stop(\"Scenario table (scenarios.rds) does not exist!\")
  }

  # Extract the full set of defined scenarios that should have been generated
  scenario_list_required <- readRDS(scenario_rds_file)
  required_scenarios <- nrow(scenario_list_required)
  if (!(required_scenarios > 0)) {
    stop(\"No scenarios stored for the experiment, scenarios.rds is empty!\")
  }

  simulation_list_created <- list.files(
    path = simulation_folder,
    pattern = \"_out.txt\"
  )
  created_simulations <- length(simulation_list_created)

  if (created_simulations == required_scenarios) {
    print(\"All OpenMalaria simulations were successfully completed.\")

    # If the missing file was previously created, remove it
    if (file.exists(missing_om_file)) {
      file.remove(missing_om_file)
    }
  } else {
    required_simulation_files <- sub(pattern = \"\\\\.xml$\", replacement = \"_out.txt\", x = scenario_list_required$file)
    ind_missing <- which(!(required_simulation_files %in% simulation_list_created))
    list_missing <- required_simulation_files[ind_missing]
    write(list_missing, missing_om_file)
    err_msg <- paste(
      \"Not all openMalaria simulations were generated.\",
      \"Number of missing simulations: \",
      length(list_missing),
      \". Check the missing_om_simulations.txt to see which\",
      \"simulations are missing and if using the cluster,\",
      \"the logs/simulation/ folder\",
      \"for the corresponding slurm job outputs.\"
    )

    if (stop_if_missing) {
      stop(err_msg)
    } else {
      print(\"WARNING:\")
      message(err_msg)
    }
  }
}

sgeCreateScenarios <- function(...) {
  system(command = paste0(
      \"qsub \", file.path(
        \"", paste0(openMalariaUtilities::getCache("experimentDir")), "\",
        \"sge_scenarios.sh\"
      )
    ))
}

sgeRunSimulation <- function(...) {
  system(command = paste0(
      \"qsub \", file.path(
        \"", paste0(openMalariaUtilities::getCache("experimentDir")), "\",
        \"sge_simulation.sh\"
      )
    ))
}

sgeRunResults <- function(...) {
  system(
    command = paste0(
      \"qsub \", file.path(
        \"", paste0(openMalariaUtilities::getCache("experimentDir")), "\",
        \"sge_results.sh\"
      )
    )
  )
}
",
file = file.path(
  openMalariaUtilities::getCache(x = "experimentDir"), "sge_run_lib.R"
),
sep = ""
)


  cat(
    "#!/usr/bin/env Rscript

expName = \"", paste0(openMalariaUtilities::getCache(x = "experimentName")), "\"

# Load experiment
start_time = Sys.time()
print(paste(Sys.time(), \"-> Loading experiment ...\"))
experiment_folder = \"", paste0(openMalariaUtilities::getCache(x = "experimentDir")), "\"

# Source lib
source(
  file.path(
        \"", paste0(openMalariaUtilities::getCache("experimentDir")), "\",
    \"sge_run_lib.R\")
)

# Create scenarios
print(paste(Sys.time(), \"-> Creating scenarios ...\"))
sgeCreateScenarios()

check_scenarios_created(experiment_folder, stop_if_missing = TRUE)

print(paste(Sys.time(), \"-> Running OpenMalaria simulations ...\"))
sgeRunSimulation()

# Check that all OpenMalaria simulations were created
check_simulations_created(experiment_folder, stop_if_missing = TRUE)

print(paste(Sys.time(), \"-> Running postprocessing ...\"))
sgeRunResults()
end_time = Sys.time()

print(paste(Sys.time(), \"-> Finished running for\"))
print(as.difftime(end_time - start_time, format = \"%X\", units = \"auto\", tz = \"CET\"))
",
file = file.path(
  openMalariaUtilities::getCache(x = "experimentDir"), "sge_run_all.R"
),
sep = ""
)
}
