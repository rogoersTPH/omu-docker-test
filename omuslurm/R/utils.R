##' @title writes a file of all job status and writes a table showing categories (failed, completed)
##' @param stime start time 00:00:00 for checking jobs ("hh:mm:ss")
##' @param sdate start date for checking jobs "yyyy-mm-dd"
##' @param AnalysisDir analysis directory folder
##' @param username slurm username
##' @importFrom magrittr %>%
##' @export
##' @return table of status of jobs on cluster
cluster_status <- function(AnalysisDir = getwd(),
                           username = as.character((Sys.info()["user"])),
                           stime = "00:00:00",
                           sdate = Sys.Date()) {
  JobName <- State <- batch <- NULL

  message(paste0("Writing job status in:\n", AnalysisDir, "/jobs.txt"))
  system(
    paste0(
      "sacct -S ", sdate, "-", stime, " -u ", username,
      " --format=JobID%30,Jobname%30,state,elapsed > ",
      file.path(AnalysisDir, "jobs.txt")
    )
  )

  message(paste("Outputting Jobs Submitted after", sdate, stime))

  jobtxt <- data.frame(utils::read.table(file.path(AnalysisDir, "jobs.txt"), header = T))
  unlink(file.path(AnalysisDir, "jobs.txt"))

  tt2 <- suppressWarnings(
    jobtxt %>%
      tidyr::separate(col = "JobID", sep = "_", into = c("batch", "id")) %>%
      dplyr::filter(
        !(JobName %in% c("batch", "dependency", "extern")) &
          !(State %in% c("CANCELLED+", "----------"))
      )
  ) # no warning
  utils::write.csv(tt2, file.path(AnalysisDir, "jobs.csv"))

  job_status <- data.frame(table(tt2[, c("batch", "State", "JobName")]))

  if (nrow(job_status) > 0) {
    job_status <- job_status[job_status$ Freq > 0, ] %>%
      dplyr::arrange(as.numeric(batch)) %>%
      dplyr::mutate(State = tolower(State))
    colnames(job_status) <- c("batch number", "status", "job name", "jobs")
  }

  return(job_status)
}

##' Cancels jobs on cluster
##' @export
cancel_jobs <- function() {
  system(
    paste0("scancel -u ", as.character((Sys.info()["user"]))),
    intern = TRUE
  )
  message("Jobs cancelled.")
  return(TRUE)
}

##' Determine which jobs failed, after running cluster_status
##' @param AnalysisDir analysis directory
##' @rdname cluster_status
##' @export
##' @importFrom magrittr %>%
##' @importFrom utils read.csv
##' @importFrom dplyr filter select
##' @note run cluster_status() to create the jobs.csv file in AnalysisDir
##' @return jobs
who_failed <- function(AnalysisDir = getwd()) {
  State <- X <- NULL
  filename <- file.path(AnalysisDir, "jobs.csv")
  if (file.exists(filename)) {
    jobs <- utils::read.csv(filename) %>%
      dplyr::filter(State %in% c("FAILED", "TIMEOUT", "NODE_FAIL")) %>%
      dplyr::select(-X)
  } else {
    stop("Run cluster_status(AnalysisDir) first,
          to create a jobs.csv file in the AnalysisDir folder.")
  }
  return(jobs)
}
