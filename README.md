# Children's Services' Data Tool

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

## Suggested Folder Structure

![Suggested Folder Structure](/docs/images/folder_structure.png)

*LA x – represents a single instance of 33 folders, one for each of the 33 LAs 

In the rest of this document, the folder structure is referenced from the root folder, LIIA project folder, using the 
following convention: 


    LIIA project folder/LA folders/{LA name}/CIN Census/Inputs 


Use of curly brackets e.g. {LA name} denotes a variable reference

### Use of the folder structure 

The diagram above is a proposed folder structure. The liiatools package is agnostic to this folder structure, so 
changes can be made to it without any code changes required. Explicit reference to the folder structure is only required 
in the inputs to the CLI commands used to run the functions within liiatools. Please see 
[Configuring liiatools](#Configuring liiatools) for guidance on how to derive these inputs. 

The only element of the folder structure created on London DataStore that must adhere to the proposal in this document 
is in the use of the names of LAs.

### Use of LA Names
The folder structure makes use of the names of the 33 LAs, in organising where LAs deposit and retrieve data, and where 
cleaned outputs are stored. The code in liiatools makes specific reference to these names. As such, you should make 
sure you use the correct LA code for each LA.  Please refer to the 
[LA code definitions](/liiatools/spec/common/LA-codes.yml) to make sure you're using the correct code.


### Installing liiatools
Liiatools can be installed using the following:

    Pip install liiatools
Or using poetry:

    poetry add liiatools 

### Configuring liiatools

All of the functions in liiatools are accessed through CLI commands. Refer to the help function for more info 

    python -m liiatools --help