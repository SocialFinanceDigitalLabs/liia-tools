# Pipeline Tools - Dependencies

## Overview

pass


## Libraries

### sf-fons-pipeline-tools

* logging
* standard data objects
  * filelocator
  * datacontainer
* archive

### sf-fons-pipeline-tools-dagster

Dagster specific utilities - such as changing the main
flow by adding stages etc.

### sfdata

Tools for additional data processing - allows use of stream parser to add detailed custom cleaning. 

### Client Implementation

Holds configuration and implementation of the client pipeline.

## sf-fons-runtime

The runtime is the main entry point for the pipeline. It is responsible for loading the configuration and running the pipeline within the sf-fons environment.

``` mermaid

graph TD;
    sf-fons-pipeline-tools --> client

    sf-fons-pipeline-tools-dagster -..-> client

    sfdata -..-> client

    client[Client Implementation] --> sf-fons-runtime;


```
