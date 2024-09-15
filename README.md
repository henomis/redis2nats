# REDIS2NATS

## Description

This project is a Redis to NATS bridge. It listens for messages on a Redis channel and forwards them to a NATS subject.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Installation

### Using Make

To build the project, run the following command:

```bash
make build
```

To run the project, run the following command:

```bash
./bin/redis2nats
```

### Using Docker

To build the Docker image, run the following command:

```bash
docker build -t redis2nats .
```

To run the Docker container, run the following command:

```bash
docker run -d --name redis2nats redis2nats
```

### Using Docker Compose

To run the Docker container using Docker Compose, run the following command:

```bash
docker-compose up redis2nats -d
```

If you want to run NATS server as well, run the following command:

```bash
docker-compose up -d
```

### Using Go

To run the project using Go, run the following command:

```bash
make run
```

## Usage

Redis2NATS has a configuration file that can be used to set the Redis and NATS connection details. The example configuration file is located at `conf/redis2nats.yaml`.

```yaml
redis:
  address: ":6379"
  maxDB: 16
nats:
  url: "nats://localhost:4222"
  bucketPrefix: "redisnats"
  timeout: "10s"
```

description of the configuration options:

- `redis.address`: The address of the Redis2NATS server.
- `redis.maxDB`: The maximum number of Redis databases.
- `nats.url`: The URL of the NATS server.
- `nats.bucketPrefix`: The prefix for the NATS bucket.
- `nats.timeout`: The timeout for the NATS connection/operations.

## Connect with the author

[![Twitter](https://img.shields.io/twitter/follow/simonevellei?label=Follow:%20Simone%20Vellei&style=social)](https://twitter.com/simonevellei) [![GitHub](https://img.shields.io/badge/Follow-henomis-green?logo=github&link=https%3A%2F%2Fgithub.com%2Fhenomis)](https://github.com/henomis) [![Linkedin](https://img.shields.io/badge/Connect-Simone%20Vellei-blue?logo=linkedin&link=https%3A%2F%2Fwww.linkedin.com%2Fin%2Fsimonevellei%2F)](https://www.linkedin.com/in/simonevellei/)

## License

© Simone Vellei, 2024~`time.Now()`
Released under the [MIT License](LICENSE)