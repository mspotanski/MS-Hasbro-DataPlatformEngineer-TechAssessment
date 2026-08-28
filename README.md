# Hasbro Data Platform Engineering Take-Home Technical Assessment

## Table of Contents
* [Section 2: Infrastructure & Data Pipeline](#section-2-infrastructure--data-pipeline)
  * [1. Prerequisites & Installation](#1-prerequisites--installation)
  * [2. AWS Credentials Configuration](#2-aws-credentials-configuration)
  * [3. Provision AWS Infrastructure (Terraform)](#3-provision-aws-infrastructure-terraform)
  * [4. Environment Variables & ETL Pipeline Execution](#4-environment-variables--etl-pipeline-execution)
  * [5. Architectural Decisions & Tradeoffs](#5-architectural-decisions--tradeoffs)
  * [6. Teardown & Resource Cleanup](#6-teardown--resource-cleanup)
* [Section 3: Analytics & Reporting](#section-3-analytics--reporting)
  * [1. Overview & Setup](#1-overview--setup)
  * [2. Execution Instructions](#2-execution-instructions)
  * [3. Analysis & Findings](#3-analysis--findings)

---

## Section 2: Infrastructure & Data Pipeline

This section provisions an AWS RDS PostgreSQL database using Terraform and executes an automated Python ETL pipeline to ingest, standardize, and load sales order records into PostgreSQL.

---

### 1. Prerequisites & Installation

Ensure the following client software packages are installed on your system before proceeding:

* **Python 3.10+**: [Python Downloads](https://www.python.org/downloads/)
* **AWS CLI**: [AWS CLI Installation Guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
* **Terraform CLI**: [Terraform Downloads](https://developer.hashicorp.com/terraform/downloads)

#### Install Python Dependencies
Open PowerShell, navigate to the project repository root directory, and install the required Python dependencies:

```powershell
pip install -r requirements.txt
```

---

### 2. AWS Credentials Configuration

The active AWS CLI profile must have an IAM policy granting adequate administrative privileges to manage RDS, VPC, and S3 resources (such as `AmazonRDSFullAccess`, `AmazonVPCFullAccess`, and `AmazonS3FullAccess`).

Configure your local AWS CLI credentials using PowerShell:

```powershell
aws configure
```

To verify your active AWS user identity, run:

```powershell
aws sts get-caller-identity
```

---

### 3. Provision AWS Infrastructure (Terraform)

Navigate to the `infra/` directory to initialize, plan, and apply the Terraform configuration for the RDS PostgreSQL database instance:

```powershell
# Navigate to Terraform directory
cd infra

# Initialize Terraform providers and modules
terraform init

# Generate and review execution plan
terraform plan

# Provision AWS infrastructure
terraform apply
```

---

### 4. Environment Variables & ETL Pipeline Execution

Return to the repository root directory, retrieve the database host endpoint and database name dynamically from Terraform state outputs, set your credentials securely, and execute the ETL pipeline script.

```powershell
# Navigate back to repository root
cd ..

# Retrieve database connection parameters from Terraform outputs
$env:PGHOST = (terraform -chdir=infra output -raw rds_endpoint)
$env:PGDATABASE = (terraform -chdir=infra output -raw rds_dbname)

# Set database master username (matches master user in infra/main.tf)
$env:PGUSER = "postgres"

# Securely prompt for database master password (masked input)
$env:PGPASSWORD = [System.Net.NetworkCredential]::new("", (Read-Host -AsSecureString "Enter PostgreSQL Password")).Password

# Execute ETL Pipeline
python pipeline/MarcusSpotanski_DataPlatformEngineer_ETL.py
```

---

### 5. Architectural Decisions & Tradeoffs

#### Data Ingestion & Auditability Strategy
* **Decision:** Preserve 100% of incoming records without dropping missing `CUSTOMER_ID` keys or altering core business values (such as negative `QUANTITY` records or out-of-bounds `DISCOUNT_PCT` values).
* **Rationale:** Data ingestion pipelines should prioritize structural standardization and schema validation over arbitrary data filtration. Dropping records during ingestion masks underlying Point-of-Sale (POS) application bugs or guest-checkout patterns. Retaining raw anomalies enables downstream data quality testing frameworks to audit issues at their source.

#### Schema Lifecycle Management
* **Decision:** Infrastructure (RDS instance, network VPC, security groups) is managed via Terraform, whereas database table DDL (`CREATE TABLE IF NOT EXISTS`) is managed dynamically inside Python upon pipeline startup.
* **Rationale:** Decouples infrastructure lifecycle tracking from application database schema management. Modifying database tables should not require running `terraform apply`, nor should infrastructure updates risk dropping live production database tables.

#### Timestamp Standardization
* **Decision:** Incoming timestamp strings are coerced into UTC standard formats and split into separate `ORDER_DATE` (`DATE`) and `ORDER_TIME` (`TIME`).
* **Rationale:** Eliminates mixed timezone format offsets while presenting standard SQL date/time types optimized for downstream analytical queries and data partitioning.

---

### 6. Teardown & Resource Cleanup

To destroy all provisioned AWS cloud resources and clear local state artifacts:

```powershell
# Navigate to Terraform directory
cd infra

# Destroy cloud infrastructure
terraform destroy

# Clear local state directory and lock files
Remove-Item -Recurse -Force .terraform, .terraform.lock.hcl, terraform.tfstate, terraform.tfstate.backup -ErrorAction SilentlyContinue
```

---

## Section 3: Analytics & Reporting

### 1. Overview & Setup
*Placeholder for Section 3 environment overview, database views, and analytical tools setup.*

### 2. Execution Instructions
*Placeholder for Section 3 reporting query execution steps and dashboard commands.*

### 3. Analysis & Findings
*Placeholder for Section 3 business metrics, data insights, and analytical reporting outputs.*
