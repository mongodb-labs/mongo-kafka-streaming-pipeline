# Using MongoDB as a Kafka Sink

### Installation

Before getting started, please make sure you have Docker installed. See the following documentation to do so: https://docs.docker.com/get-docker/

```sh
git clone https://github.com/google/cadvisor.git
brew install coreutils
brew install jq
```

To test different load profiles (i.e. throughput vs message size), run the tests with the following command below.
```sh
./run_test_suite.sh
```

### Known Limitations
- All three primary components (Kafka, Connect & MongoDB) were given the same resources:
    - 2 CPU
    - 2GB of memory
- No performance tuning or optimisation took place - all components were used out of the box.
- Apache Kafka’s internal performance testing script (***kafka-producer-perf-test.sh***) was used to simulate the different load profiles. This script is executed within Kafka’s broker container which may impact the broker’s performance numbers.
- The setup only measures Kafka Connect as a sink. Using Kafka Connect as a source was not measured.