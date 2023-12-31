% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/calibration.R
\name{plotQuadraticApprox_c}
\alias{plotQuadraticApprox_c}
\title{Plot quadratic approximation and smoothing}
\usage{
plotQuadraticApprox_c(quadratic_l_approx, list_setting = NULL, list_sub = NULL)
}
\arguments{
\item{quadratic_l_approx}{Output from the function get_quadratic_fit_cf}

\item{list_setting}{Settings to choose from}

\item{list_sub}{Sub settings to choose from}
}
\value{
A ggplot object
}
\description{
Plot smoothing and quadratic approximation of likelihood profiles
following Ionides et al. 2017
}
