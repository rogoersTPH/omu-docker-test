##' @title Create script for SLURM submission
##' @export
slurmPrepareRun <- function() {

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
    required_simulation_files <- sub(pattern = \"\\.xml$\", replacement = \"_out.txt\", x = scenario_list_required$file)
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

slurmCreateScenarios <- function(moreArgs = NULL, n = 0, limit = 3, ...) {
  if (n < limit) {
    message(\"(Re)Submitting jobs... \")
    n <- n + 1
    jobID <- system(command = paste0(
      \"sbatch --wait \", if (!is.null(moreArgs)) {
        paste0(moreArgs, \" \", collapse = \" \")
      }, file.path(
        \"", paste0(openMalariaUtilities::getCache("experimentDir")), "\",
        \"slurm_scenarios.sh\"
      )
    ), intern = TRUE)

    jobID <- as.numeric(
      gsub(
        pattern = \"Submitted batch job ([0-9]+)\", replacement = \"\\\\1\", x = jobID
      )
    )

    status <- utils::read.table(
      text = system(
        paste0(
          \"sacct -j \", jobID,
          \" --format JobID,State --noheader --parsable2 --noconvert\"
        ),
        intern = TRUE, ignore.stderr = TRUE
      ), header = FALSE, sep = \"|\"
    )

    ## Filter out .batch/.extern substrings
    status <- status[!grepl(pattern = \"[0-9]+_[0-9]+\\..+\", x = status$V1), ]
    ## Keep only failed jobs
    fails <- status[status$V2 %in% c(\"BOOT_FAIL\", \"NODE_FAIL\", \"FAILED\"), ]
    ## Get the batch numbers
    fails$V3 <- as.numeric(gsub(
      pattern = \"[0-9]+_([0-9]+)\", replacement = \"\\\\1\", x = fails$V1
    ))
    new_runs <- fails$V3
    ## Inform about non-failed or completed jobs
    others <- status[!status$V2 %in% c(\"BOOT_FAIL\", \"NODE_FAIL\", \"FAILED\", \"COMPLETED\", \"PENDING\", \"RUNNING\"), ]
    if (nrow(others) > 0) {
      message(\"Please check the state of the following jobs:\n\")
      print(data.frame(JobID = others$V1, State = others$V2))
      message(\"\")
    }
    if (length(new_runs > 0)) {
      message(
        paste0(
          \"Trying to run the following jobs again: \",
          paste0(fails$V1, collapse = \", \")
        )
      )
      message(paste0(limit - n, \" tries left\"))
      ## Prepare new runs
      old_f <- readLines(file.path(
        \"", paste0(openMalariaUtilities::getCache("experimentDir")), "\",
        \"slurm_scenarios.sh\"
      ))
      new_f <- gsub(
        pattern = \"#SBATCH --array=.*\",
        replacement = paste0(\"#SBATCH --array=\", paste0(new_runs, collapse = \",\")),
        x = old_f
      )
      writeLines(new_f, con = file.path(
        \"", paste0(openMalariaUtilities::getCache("experimentDir")), "\",
        \"slurm_scenarios.sh\"
      ))
      slurmCreateScenarios(moreArgs = moreArgs, n = n, limit = limit)
    } else {
      message(\"All jobs submitted!\")
      return(TRUE)
    }
  } else {
    stop(\"Some jobs have failed!\")
  }
}

slurmRunSimulation <- function(moreArgs = NULL, n = 0, limit = 3, ...) {
  if (n < limit) {
    message(\"(Re)Submitting jobs... \")
    n <- n + 1
    jobID <- system(command = paste0(
      \"sbatch --wait \", if (!is.null(moreArgs)) {
        paste0(moreArgs, \" \", collapse = \" \")
      }, file.path(
        \"", paste0(openMalariaUtilities::getCache("experimentDir")), "\",
        \"slurm_simulation.sh\"
      )
    ), intern = TRUE)

    jobID <- as.numeric(
      gsub(
        pattern = \"Submitted batch job ([0-9]+)\", replacement = \"\\\\1\", x = jobID
      )
    )

    status <- utils::read.table(
      text = system(
        paste0(
          \"sacct -j \", jobID,
          \" --format JobID,State --noheader --parsable2 --noconvert\"
        ),
        intern = TRUE, ignore.stderr = TRUE
      ), header = FALSE, sep = \"|\"
    )

    ## Filter out .batch/.extern substrings
    status <- status[!grepl(pattern = \"[0-9]+_[0-9]+\\..+\", x = status$V1), ]
    ## Keep only failed jobs
    fails <- status[status$V2 %in% c(\"BOOT_FAIL\", \"NODE_FAIL\", \"FAILED\"), ]
    ## Get the batch numbers
    fails$V3 <- as.numeric(gsub(
      pattern = \"[0-9]+_([0-9]+)\", replacement = \"\\\\1\", x = fails$V1
    ))
    new_runs <- fails$V3
    ## Inform about non-failed or completed jobs
    others <- status[!status$V2 %in% c(\"BOOT_FAIL\", \"NODE_FAIL\", \"FAILED\", \"COMPLETED\", \"PENDING\", \"RUNNING\"), ]
    if (nrow(others) > 0) {
      message(\"Please check the state of the following jobs:\n\")
      print(data.frame(JobID = others$V1, State = others$V2))
      message(\"\")
    }
    if (length(new_runs > 0)) {
      message(
        paste0(
          \"Trying to run the following jobs again: \",
          paste0(fails$V1, collapse = \", \")
        )
      )
      message(paste0(limit - n, \" tries left\"))
      ## Prepare new runs
      old_f <- readLines(file.path(
        \"", paste0(openMalariaUtilities::getCache("experimentDir")), "\",
        \"slurm_simulation.sh\"
      ))
      new_f <- gsub(
        pattern = \"#SBATCH --array=.*\",
        replacement = paste0(\"#SBATCH --array=\", paste0(new_runs, collapse = \",\")),
        x = old_f
      )
      writeLines(new_f, con = file.path(
        \"", paste0(openMalariaUtilities::getCache("experimentDir")), "\",
        \"slurm_simulation.sh\"
      ))
      slurmRunSimulation(moreArgs = moreArgs, n = n, limit = limit)
    } else {
      message(\"All jobs submitted!\")
      return(TRUE)
    }
  } else {
    stop(\"Some jobs have failed!\")
  }
}

slurmRunResults <- function(moreArgs = NULL, ...) {
  system(
    command = paste0(
      \"sbatch --wait \", if (!is.null(moreArgs)) {
        paste0(moreArgs, \" \", collapse = \" \")
      }, file.path(
        \"", paste0(openMalariaUtilities::getCache("experimentDir")), "\",
        \"slurm_results.sh\"
      )
    )
  )
}
",
file = file.path(
  openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_lib.R"
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
    \"slurm_run_lib.R\")
)

# Create scenarios
print(paste(Sys.time(), \"-> Creating scenarios ...\"))
slurmCreateScenarios()

check_scenarios_created(experiment_folder, stop_if_missing = TRUE)

print(paste(Sys.time(), \"-> Running OpenMalaria simulations ...\"))
slurmRunSimulation()

# Check that all OpenMalaria simulations were created
check_simulations_created(experiment_folder, stop_if_missing = TRUE)

print(paste(Sys.time(), \"-> Running postprocessing ...\"))
slurmRunResults()
end_time = Sys.time()

print(paste(Sys.time(), \"-> Finished running for\"))
print(as.difftime(end_time - start_time, format = \"%X\", units = \"auto\", tz = \"CET\"))
",
file = file.path(
  openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_all.R"
),
sep = ""
)
}
