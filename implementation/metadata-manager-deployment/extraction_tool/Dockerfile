FROM r-base:4.1.1

RUN apt-get update \
 && apt-get -y install cron \
                       build-essential libcurl4-gnutls-dev libxml2-dev libssl-dev \
 && rm -rf /var/lib/apt/lists/*

RUN Rscript -e 'if (!require("devtools")) install.packages("devtools") ; devtools::install_github("EHDEN/CatalogueExport")'
