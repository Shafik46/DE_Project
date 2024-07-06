# Airflow Project

This project contains an Airflow setup for orchestrating a data pipeline that scrapes web data, processes it, validates the data, and loads it into a PostgreSQL database. The entire setup is containerized using Docker.

## Prerequisites

- Docker: Ensure you have Docker installed. You can download it from [Docker's official website](https://www.docker.com/get-started).
- Docker Compose: Ensure you have Docker Compose installed. Docker Desktop includes Docker Compose.

## Minimum Requirements

- Docker should have at least **6GB of RAM** allocated. You can adjust this in Docker Desktop under `Preferences > Resources`.

## Setup Instructions

### 1. Clone the Repository

```sh
git clone https://github.com/Shafik46/DE_Project.git
cd DE_Project
```

### 2. Environment Configuration

Ensure your environment variables are set correctly in the Docker Compose file. Here's an example configuration:

```yaml
version: '3'
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"

  airflow:
    image: apache/airflow:2.2.5
    depends_on:
      - postgres
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      - AIRFLOW__CORE__FERNET_KEY=''
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
      - AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.basic_auth
      - AIRFLOW_CONN_POSTGRES_DEFAULT=postgres://airflow:airflow@postgres:5432/airflow
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
    ports:
      - "8080:8080"
    command: >
      bash -c "airflow db init && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com && airflow scheduler & airflow webserver"
```

### 3. Build and Start the Docker Containers

```sh
docker-compose up --build
```

This command will build the Docker images and start the containers. The Airflow web server will be accessible at `http://localhost:8080`.

### 4. Access Airflow

- Open your web browser and navigate to `http://localhost:8080`.
- Log in using the credentials:
  - Username: `airflow`
  - Password: `airflow`

### 5. Stopping the Containers

To stop the containers, run:

```sh
docker-compose down
```

## Notes

- Ensure that Docker has at least **6GB of RAM** allocated. You can adjust this in Docker Desktop under `Preferences > Resources`.
- If you encounter any issues, refer to the Docker and Airflow logs for troubleshooting.


### Summary

- **Clone the Repository**: Provides instructions to clone the repository.
- **Environment Configuration**: Details on setting up the Docker Compose environment.
- **Build and Start Containers**: Instructions to build and start the Docker containers.
- **Access Airflow**: How to access the Airflow web interface.
- **Stopping the Containers**: How to stop the running containers.
- **Notes**: Additional notes on resource requirements and troubleshooting.

By following these steps, users will be able to set up and run the Docker containers for your Airflow project locally.
