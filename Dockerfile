# Build OpenMalaria
FROM docker.io/rocker/r-ver:4.3 as build
WORKDIR /
RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential ca-certificates git libz-dev python3 cmake libgsl-dev libxerces-c-dev xsdcxx libboost-dev \
  && rm -rf /var/lib/apt/lists/* \
  && git clone --depth 1 --branch schema-44.0 https://github.com/SwissTPH/openmalaria.git \
  && cd openmalaria \
  && mkdir build \
  && cd build \
  && cmake -DCMAKE_BUILD_TYPE=Release .. \
  && make -j8 \
  && cd .. \
  && mkdir openmalariaRelease \
  && cp build/openMalaria openmalariaRelease/ \
  && cp build/schema/scenario_current.xsd openmalariaRelease/scenario_44.xsd \
  && cp build/schema/scenario_current.xsd openmalariaRelease/ \
  && cp test/*.csv openmalariaRelease/

# Install OpenMalaria and dependencies
FROM docker.io/rocker/r-ver:4.3 as deploy
WORKDIR /om
RUN apt-get update && apt-get install -y --no-install-recommends \
  libgsl27 libxerces-c3.2 libmysqlclient21 sqlite3 \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build /openmalaria/openmalariaRelease/* ./
RUN ln -sfv /om/openMalaria /usr/local/bin/openMalaria

# Install and setup R environment
WORKDIR /omu
ARG RENV_VERSION=v1.0.0

RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cloud.r-project.org'))" \
    && R -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')" \
    && mkdir -p renv

COPY renv.lock renv.lock
ENV RENV_PATHS_LIBRARY renv/library

# Set up renv and restore packages.
# Using an explicit library path helps make this more portable across Docker and Singularity.
COPY omucompat ./omucompat
COPY omuslurm ./omuslurm
COPY omuaddons ./omuaddons

RUN R -e "renv::restore()" \
    && R -e "renv::install('SwissTPH/r-openMalariaUtilities', ref = 'v23.02')" \
    && R -e "renv::install('SwissTPH/AnophelesModel')" \
    && R -e "renv::install('tidyverse')" \
    && R -e "renv::install('broom')" \
    && R -e "renv::install('extraDistr')" \
    && R -e "renv::install('ggthemes')" \
    && R -e "renv::install('./omucompat')" \
    && R -e "renv::install('./omuslurm')" \
    && R -e "renv::install('./omuaddons')" \
    && R -e "renv::snapshot()"


# Default command to launch R
CMD ["Rscript"]
