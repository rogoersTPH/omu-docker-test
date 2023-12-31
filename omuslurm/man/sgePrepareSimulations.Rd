% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sge_run.R
\name{sgePrepareSimulations}
\alias{sgePrepareSimulations}
\title{Run preparations for SGE submission}
\usage{
sgePrepareSimulations(
  expName,
  scenarios,
  bSize = 200,
  memCPU = "1G",
  nCPU = 1,
  time = "06:00:00",
  qos = "all.q",
  verbose = FALSE,
  ...
)
}
\arguments{
\item{expName}{Name of experiment}

\item{scenarios}{Scenario data frame}

\item{bSize}{Number of parrellel processed files}

\item{memCPU}{Memory per CPU}

\item{nCPU}{Number of cores to reserve}

\item{time}{Maximum time}

\item{qos}{Quality of service}

\item{verbose}{If TRUE, use OpenMalaria's verbose output.}
}
\description{
Run preparations for SGE submission
}
