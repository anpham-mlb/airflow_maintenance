# Introduction

Note: This project is a small part of a scraping application built using Airflow and Python. It automates the process of fetching data from a live API and storing it in a database on Google Cloud Platform (GCP). Airflow is used to schedule and manage the tasks, ensuring that the data is regularly updated on a weekly basis.

# Description

- Automated data scraping from a live API
- Task scheduling and management with Airflow
- Data storage and refreshing on Google Cloud Platform (GCP)

## Requirements

To run this project locally, you need to have the latest versions for following prerequisites installed:

- Python
- Apache Airflow
- Docker

# Usage

1. Configure the necessary settings for your GCP credentials, API endpoints, and other environment-specific variables.

2. Set up Docker to run Airflow locally

3. Start the Airflow scheduler and web server

4. Access the Airflow web UI by visiting `http://localhost:8080` in your browser.

5. Set up the necessary DAGs (Directed Acyclic Graphs) in Airflow to define your scraping tasks and schedule them accordingly.

# Configuration

Before running the project, you need to configure the following settings:

- GCP credentials: Obtain the necessary credentials from GCP and set them up on your local environment.

- API endpoints: Specify the relevant API endpoints that you want to scrape data from. Update the corresponding configurations in the project files.

- Airflow DAGs: Define your scraping tasks as DAGs in the `dags/` directory. Customize the scheduling intervals according to your needs.

- Other environment-specific variables: Adjust any additional settings or environment variables as required.







