% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/postprocessing-db.R
\name{omOutputDict_old}
\alias{omOutputDict_old}
\title{Open Malaria output dictionary}
\usage{
omOutputDict_old()
}
\description{
A dictionary which provides translations for the following
outputs of Open Malaria:
\itemize{
\item Survey measure numbers to names
\item Whether measures are summed up between survey dates (incident = TRUE) or
represent prevalent characteristics (incident = FALSE)
\item An identifier for the 'third dimension' column. This can be 'age_group',
'vector_species', 'drug_id' or NA
}

See: https://github.com/SwissTPH/openmalaria/wiki/MonitoringOptions
To parse measure names from openMalaria v44.0 source code:
grep -oP '[\K[^]]+' model/mon/OutputMeasures.h > tmp1.txt ;
grep 'OutMeasure::' model/mon/OutputMeasures.h | cut -d, -f 2 > tmp2.txt
cat tmp2.txt | while read line;do grep -r "mon::$line" model/Host model/interventions/ model/Clinical model/Transmission model/PkPd | grep -Po 'report\K[^(]' | paste -s -d, - >> tmp3.txt;done
paste tmp.txt > surveyMeasureTable.txt
rm tmp*.txt

[^]: R:\%5E\%5C
[^(]: R:\%5E(
}
