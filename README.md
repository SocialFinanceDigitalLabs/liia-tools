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

