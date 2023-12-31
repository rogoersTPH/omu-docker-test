% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/postprocessing-db.R
\name{postprocess}
\alias{postprocess}
\title{Wrapper function to do postprocessing}
\usage{
postprocess(
  experimentDir = NULL,
  dbName = NULL,
  experiment_id = NULL,
  scenario_id = NULL,
  batch_id = NULL,
  readResults = F,
  metadataFeatures = NULL,
  indicators = c("incidence", "prevalence"),
  customIndicatorFormula = NULL,
  keepMeasuresForIndicators = NULL,
  aggregateByAgeGroup = NULL,
  aggregateByDate = NULL,
  wideFormat = T,
  timeHorizon = c("2000-01-01", "2100-01-01"),
  outputFilename = NULL
)
}
\arguments{
\item{experimentDir}{Experiment directory, if null then use current working directory}

\item{dbName}{Name of the sqlite file, if null then use name of rootDir}

\item{experiment_id}{Number of experiment id to process}

\item{scenario_id}{Numeric vector of scenario ids to process, if NULL then process all scenarios within given experiment}

\item{batch_id}{Numeric, adds batch_id to processed data if NULL then nothing is added}

\item{readResults}{Logical, if TRUE extract data and overwrites sqlite file while creating data base from output files}

\item{metadataFeatures}{Character vector of metadata features (patterns are matched) to keep, if null keep all}

\item{indicators}{Character vector of epidemiological indicators to calculate}

\item{customIndicatorFormula}{Character vector to parse and evaluate for custom indicators}

\item{keepMeasuresForIndicators}{Character vector of indicators whose measures we want to keep}

\item{aggregateByAgeGroup}{Character vector of age groups (e.g. "2-10" for the
right-open interval) to be
used for aggregation, if NULL, then age groups from surveys will be used}

\item{aggregateByDate}{List (named "month", "year", "date") list(month="month",year="year",date=as.Date(c("2010-04-06","2012-04-06"))) to
be used for aggregation, if NULL, then survey dates will be used, for month, aggregated survey date is set to middle of month}

\item{wideFormat}{Logical vector, if TRUE than output in wide format}

\item{timeHorizon}{Character vector of start date and end date of
time horizon to be processed, by default start on 2000-01-01}

\item{outputFilename}{Character vector file name for processed data (.rds and .csv allowed), if NULL then write to epidemiologicalIndicators.rds in rootDir}
}
\description{
Wrapper function to do postprocessing
}
