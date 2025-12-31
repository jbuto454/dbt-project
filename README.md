# Haunted Houses Analytics - dbt Project

A dbt project modeling ticket sales and customer feedback data for a haunted house theme park, demonstrating data transformation best practices and dimensional modeling.

## Project Overview

This project transforms raw operational data into analytics-ready dimensional models using dbt and Snowflake. The data covers haunted house attractions, ticket purchases, and customer feedback.

## Architecture

```
Sources (RAW)          Staging (STAGING)           Marts (MART)
─────────────          ─────────────────           ────────────
raw_customers      →   stg_customers           →   dim_customers
raw_haunted_houses →   stg_haunted_houses      →   dim_haunted_houses
raw_haunted_house_tickets → stg_haunted_house_tickets → fact_visits
raw_customer_feedbacks → stg_customer_feedbacks  ↗
valid_domains      →   stg_valid_domains       ↗
```

## Key Transformations

### Staging Layer
- **Deduplication**: Uses `row_number()` window functions to handle duplicate records, keeping the most recent version of each entity
- **Data typing**: Ensures consistent column naming and prepares data for downstream consumption
- **Audit columns**: Adds `loaded_at` timestamps for data lineage tracking

### Marts Layer

**dim_customers**
- Enriches customer data with email validation
- Extracts email domains and validates against known valid domains
- Adds `is_valid_email` boolean flag using a left join pattern

**dim_haunted_houses**
- Converts house size from square feet to square meters
- Preserves original units alongside converted values

**fact_visits**
- Joins ticket purchases with customer feedback
- Uses left join to preserve all ticket records, with nullable feedback fields
- Combines transactional data (tickets) with qualitative data (ratings, comments)

## Data Quality & Documentation

### Source Configuration
Sources are defined in `_sources.yml`, connecting dbt models to raw Snowflake tables:
- Database: `PORTFOLIO_DB`
- Schema: `RAW`

### Testing Strategy
Each model includes yml schema files with:
- **Uniqueness tests** on primary keys (`customer_id`, `ticket_id`, `haunted_house_id`, `feedback_id`)
- **Not null tests** on required fields
- **Column-level documentation** describing business context

Example test configuration:
```yaml
columns:
  - name: customer_id
    description: "Unique identifier for each customer."
    data_tests:
      - unique
      - not_null
```

### Nullable Fields
The `fact_visits` table intentionally allows nulls on `comments` and `rating` columns since not all ticket purchases have associated feedback (right join preserves all tickets).

## Project Structure

```
haunted_houses/
├── models/
│   ├── staging/
│   │   ├── _sources.yml
│   │   ├── stg_customers.sql
│   │   ├── stg_customers.yml
│   │   ├── stg_customer_feedbacks.sql
│   │   ├── stg_customer_feedbacks.yml
│   │   ├── stg_haunted_houses.sql
│   │   ├── stg_haunted_houses.yml
│   │   ├── stg_haunted_house_tickets.sql
│   │   ├── stg_haunted_house_tickets.yml
│   │   ├── stg_valid_domains.sql
│   │   └── stg_valid_domains.yml
│   └── marts/
│       ├── dim_customers.sql
│       ├── dim_customers.yml
│       ├── dim_haunted_houses.sql
│       ├── dim_haunted_houses.yml
│       ├── fact_visits.sql
│       └── fact_visits.yml
├── seeds/
│   ├── raw_customers.csv
│   ├── raw_customer_feedbacks.csv
│   ├── raw_haunted_houses.csv
│   ├── raw_haunted_house_tickets.csv
│   └── valid_domains.csv
├── macros/
│   └── generate_schema_name.sql
└── dbt_project.yml
```

## Tech Stack

- **dbt**: v1.11.2
- **Data Warehouse**: Snowflake
- **Transformation Language**: SQL

## Setup & Usage

### Prerequisites
- Python 3.8+
- Snowflake account
- dbt-snowflake adapter

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/haunted-houses-dbt.git
cd haunted-houses-dbt

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install dbt-snowflake

# Configure profiles.yml with your Snowflake credentials
```

### Running the Project

```bash
# Install dbt packages
dbt deps

# Load seed data
dbt seed

# Run all models

dbt run

# Run tests
dbt test

# Or run everything at once
dbt build
```

## Skills Demonstrated

- Dimensional modeling (facts and dimensions)
- SQL window functions for deduplication
- Join strategies (left, right, inner)
- Data quality testing with dbt
- Model and column documentation
- Schema management with custom macros
- Unit conversions and data enrichment
