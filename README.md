# Rollercoaster Tycoon Database

## Description
This project provides scripts to create, populate and manage a relational database containing information about theme parks, rides, shops, stalls, foods and items. It handles the creation of tables, insertion of seed data, and setting up foreign key relationships, including many-to-many bridge tables.


## Technologies used
- **Python:** Version 3.12
- **Database:** PostgreSQL (or any SQL-compatible database)
- **Data Formats:** Python dictionaries in src/data for seeding
- **Automation Tools:**  CI/CD for deployment

## Installation

### Prerequisites
- [Python](https://www.python.org/downloads/) - version 3.12 or above
- [Make](https://www.gnu.org/software/make/)
- [PostgreSQL](https://www.postgresql.org/docs/l)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/ScrappyNozicka/Rollercoaster-Tycoon-Database.git
   cd Rollercoaster-Tycoon-Database
2. Install dependencies:
    - `create-environment`(automated by `requirements`): Creates a Python virtual environment.
        ```bash
        make create-environment
    - `requirements`: Installs the project dependencies from requirements.txt.
        ```bash
        make requirements
    - `dev-setup`: Installs development tools(bandit, black, flake8, pytest-cov, and pip-audit)
        ```bash
        make dev-setup
    - `run-checks`: Runs security tests, code checks, unit tests, coverage analysis
        ```bash

        make run-checks
