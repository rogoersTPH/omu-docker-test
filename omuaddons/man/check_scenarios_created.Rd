% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/check_workflow.R
\name{check_scenarios_created}
\alias{check_scenarios_created}
\title{checks if the defined scenarios for an experiment were created}
\usage{
check_scenarios_created(experiment_folder, stop_if_missing = FALSE)
}
\arguments{
\item{experiment_folder}{folder with the workflow outputs of an experiment}

\item{stop_if_missing}{if TRUE the function returns an error message and
stops further downstream operations if any scenarios are missing;
default value is FALSE}
}
\value{
if there are missing scenarios, it creates a file,
missing_scenarios.txt, containing the file names of the missing scenarios
}
\description{
checks if the defined scenarios for an experiment were created
}
