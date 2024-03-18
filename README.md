# Children's Services' Data Tool

![Unit Tests](https://github.com/SocialFinanceDigitalLabs/liia-tools/actions/workflows/tests.yml/badge.svg)
[![codecov](https://codecov.io/github/SocialFinanceDigitalLabs/liia-tools/graph/badge.svg?token=R1YSMXDX1B)](https://codecov.io/github/SocialFinanceDigitalLabs/liia-tools)

This repository holds a set of tools and utilities for processing and cleaning Children's Services' data.

Most of the utilities are centred around three core datasets:

* SSDA903
* CIN Census
* Annex A

## CIN Census

The CIN Census file is provided as one or more XML files. The core tools allow you to validate both an entire CIN Census
file, or validate individual Child elements and flag or discard those that do not conform with
the [CIN Census schema](liiatools/spec/cin/cin-2022.xsd).

In addition, the tool can detect and conform non-ISO8601 dates to the correct format and reject incorrect values from
enumerations. 



# liiatools

This document is designed as a user guide for installing and configuring the liiatools PyPI package released to 
London DataStore for the processing of Children’s Services datasets deposited by London Local Authorities 

## Introduction to LIIA project 

The LIIA (London Innovation and Improvement Alliance) project brings together Children’s Services data from all the 
Local Authorities (LAs) in London with the aim of providing analytical insights that are uniquely possible using 
pan-London datasets. 

 
Please see [LIIA Child Level Data Project](https://liia.london/liia-programme/targeted-work/child-level-data-project) 
for more information about the project, its aims and partners. 

## Purpose of liiatools package 

The package is designed to process data deposited onto the London DataStore by each of the 33 London LAs such that it 
can be used in for analysis purposes. The processing carries out the following tasks: 

* validates the deposited data against a specification 
* removes information that does not conform to the specification
* degrades sensitive information to reduce the sensitivity of the data that is shared 
* exports the processed data in an analysis-friendly format for: 
  * the LA to analyse at a single-LA level 
  * additional partners to analyse at a pan-London level 

The package is designed to enable processing of data from several datasets routinely created by all LAs as part of 
their statutory duties. In v0.1 the datasets that can be processed are: 

* [Annex A](/liiatools/datasets/annex_a/README.md)
* CIN (Future Release)
* 903 (Future Release)

The package is designed to process data that is deposited by LAs into a folder directory to be created on the London 
DataStore for this purpose.Page Break 

### Installing liiatools
Liiatools can be installed using the following:

    Pip install liiatools
Or using poetry:

    poetry add liiatools 

### Configuring liiatools

All of the functions in liiatools are accessed through CLI commands. Refer to the help function for more info 

    python -m liiatools --help


# liia-code-server
This is a Dagster code server library which is setup to be used as a code server.

## How to use:

### Local Development
1. Run `poetry install`
2. Copy `.env.sample` to `.env` and fill in the variables there as needed
3. Run the following command: `poetry run dagster dev -f .\liia\repository.py`
4. Once running, navigate to http://localhost:3000/
5. Add the pre-commit hook by running `pre-commit install`. This will ensure your code is formatted before you commit something
   
### Preparation for Production or Staging
How this will run in production is that the library will be brought into a docker container
with configuration specified in the file `Dockerfile_user_code`.  Which code servers are used can
be specified in the installation. 
See [The SFDATA Platform's Workspace definition for details](https://github.com/SocialFinanceDigitalLabs/sfdata-platform/blob/main/dagster/workspace.yaml)

The idea is each code server will have its own setup which will be a copy of what's here.

Note: Multiple libraries, pipelines, etc can exist in a single code server. Different servers should
be used if they have conflicting requirements (e.g. different python versions)

