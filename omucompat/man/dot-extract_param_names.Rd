% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/compat.R
\name{.extract_param_names}
\alias{.extract_param_names}
\title{Function to extract names of parameters}
\usage{
.extract_param_names(
  full,
  scens,
  models = "models",
  seed = "seed",
  fut = "fut",
  placeholder = NULL,
  seed_as_hist_param = TRUE,
  include = NULL
)
}
\arguments{
\item{full}{List of experiment variables and values}

\item{scens}{Dataset of scenarios}

\item{models}{Name is "models"}

\item{seed}{Name is "seed"}

\item{fut}{Assumes all future interventions have this specific pattern (i.e.
'fut') in their name}

\item{placeholder}{Variables that are not needed in either FutScen or
HistScen}

\item{seed_as_hist_param}{If TRUE, then seed is a HistScen_param}

\item{include}{Variables that are not in full but are needed to distinguish
scenarios}
}
\description{
Function to extract names of parameters
}
\note{
Used often internally, never seen by user, easy to write, rarely
breaks
}
