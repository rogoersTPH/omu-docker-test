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
  libgsl27 libxerces-c3.2 \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build /openmalaria/openmalariaRelease/* ./

# Install and setup R environment
WORKDIR /omu
ARG RENV_VERSION=v1.0.0
RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cloud.r-project.org'))" \
    && R -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')" \
    && mkdir -p renv
COPY renv.lock renv.lock
COPY .Rprofile .Rprofile
COPY renv/activate.R renv/activate.R
COPY renv/settings.json renv/settings.json
RUN R -e "renv::restore()" \
    && R -e "renv::install('SwissTPH/r-openMalariaUtilities', ref = 'v23.02')"
