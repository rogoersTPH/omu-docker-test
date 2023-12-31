% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/check_workflow.R
\name{check_simulations_created}
\alias{check_simulations_created}
\title{checks if the OpenMalaria simulations for an experiment were
successfully completed}
\usage{
check_simulations_created(experiment_folder, stop_if_missing = FALSE)
}
\arguments{
\item{experiment_folder}{folder with the workflow outputs of an experiment}

\item{stop_if_missing}{if TRUE the function returns an error message and
stops further downstream operations if any simulations failed;
default value is FALSE}
}
\value{
if there are missing simulations, it creates a file,
missing_om_simulations.txt, containing the file names of
the failed simulations
}
\description{
checks if the OpenMalaria simulations for an experiment were
successfully completed
}
