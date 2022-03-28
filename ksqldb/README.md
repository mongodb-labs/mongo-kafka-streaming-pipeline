# Using MongoDB Change Streams as a ELT pipeline

### Installation

Before getting started, please make sure you have Docker installed. See the following documentation to do so: https://docs.docker.com/get-docker/


Start Docker containers and add connectors.
```sh
./start-up.sh
```

Install node modules and go to src.
```sh
yarn
cd src
```

Build the sample materialized view.
```sh
node buildMaterializedViews.js
```

Listen to changes and update materialized view.
```sh
node changeStream.js
```

### Known Limitations
- This project did not investigate all use cases of aggregation pipelines and only chose a few common use cases.
- The code provided is meant to be used as a point of reference and should only be used as such.