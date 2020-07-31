# PMC Tables

[![conda](https://img.shields.io/conda/dn/ostrokach/pmc_tables.svg)](https://anaconda.org/ostrokach/pmc_tables/)
[![docs](https://img.shields.io/badge/docs-v0.1.4.dev0-blue.svg)](https://ostrokach.gitlab.io/pmc_tables/)
[![build status](https://gitlab.com/ostrokach/pmc_tables/badges/master/pipeline.svg)](https://gitlab.com/ostrokach/pmc_tables/commits/master/)
[![coverage report](https://gitlab.com/ostrokach/pmc_tables/badges/master/coverage.svg)](https://gitlab.com/ostrokach/pmc_tables/commits/master/)

Extract relational tables from PubMed Central.

## Features

- Download articles and supplemental material from NCBI PubMed Central and EuroPMC.
- Extract tables from articles and supplemental material and save then in HDF5 files.

## Notebooks

This repository contains the pipeline for extracting and processing tables from [PubMed Central](https://www.ncbi.nlm.nih.gov/pmc) and [Europe PMC](https://europepmc.org/).

```mermaid
graph TD;
  statistics.ipynb --> download.ipynb;
  to_download.ipynb --> download.ipynb;
  download.ipynb --> archive_to_hdf5.ipynb;
  archive_to_hdf5.ipynb --> query_mutation_tables.ipynb;
  presentation.ipynb;
  project_report.ipynb;
```

## Additional resources

- [Data package](https://gitlab.com/datapkg/pmc-tables-pipeline) - Repository of Jupyter notebooks showing how to run the entire PMC tables pipeline.
- [Project report](https://gitlab.com/strokach/courses/CSC2525/blob/master/project/paper.pdf) - Project report in which I describe PMC tables.
- [Project presentation](https://docs.google.com/presentation/d/1oSOegvLNX5IsO4RpASwptsjSnoC9DsfBDnIT908QOZU/edit?usp=sharing) - Slides for a course presentation in which I describe PMC tables.
