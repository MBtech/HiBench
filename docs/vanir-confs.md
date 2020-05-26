# Configurations for the experiments in Vanir

## Benchmarks under the `sparkbench` directory 

### Scale parameter
- LPA: Small
- CC: Small
- SP: Small
- LR: Gigantic
- Nweight: Gigantic
- PageRank: Gigantic
- GBT: Huge
- RF: Huge

### Modifications to the features represented by the Scale parameter
Some of the configurations for the corresponding scale values were modified in order to make the benchmarks execute in a decent enough length of time over the configuration space selected for evaluation. If a benchmark is not mentioned below then the default values for configurations corresponding to the scale parameter were used. These values can be found below:

- LPA: Number of Edges set to 500000
- CC: Number of Edges set to 3500000
- SP: Number of Edges set to 2750000
- PageRank: Number of pages set to 15000000
- RF: Number of examples set to 20000

## Benchmarks under `pipeline-benchmarks` directory
`housing-prices` directory contains the `price prediction(pp)` benchmark that is used in Vanir.

The initial number of records in the base data is set to `10,000` and the number of records in the input data processed by the ETL stage is set to `5,000,000`. 
