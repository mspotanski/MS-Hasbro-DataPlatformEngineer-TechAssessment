# Hasbro Data Platform Engineering Take-Home Technical Assessment

## Table of Contents
* [Section 1: Technical Documentation](#section-1-technical-documentation)
* [Section 2: Data Pipelines; Terraform and AWS Postgres RDS](#section-2-data-pipelines-terraform-and-aws-postgres-rds)
  * [1. Prerequisites & Installation](#1-prerequisites--installation)
  * [2. AWS Credentials Configuration](#2-aws-credentials-configuration)
  * [3. Provision AWS Infrastructure (Terraform)](#3-provision-aws-infrastructure-terraform)
  * [4. Environment Variables & ETL Pipeline Execution](#4-environment-variables--etl-pipeline-execution)
  * [5. Architectural Decisions & Tradeoffs](#5-architectural-decisions--tradeoffs)
  * [6. Teardown & Resource Cleanup](#6-teardown--resource-cleanup)
* [Section 3: ML API; Secure Iris Classifier API with FASTAPI and ngrok](#section-3-ml-api-secure-iris-classifier-api-with-fastapi-and-ngrok)
  * [1. Architecture & Technical Decisions](#1-architecture--technical-decisions)
  * [2. Troubleshooting Note: Python Versioning](#2-troubleshooting-note-python-versioning)
  * [3. Setup & Installation](#3-setup--installation)
  * [4. Running the Application](#4-running-the-application)

---

## Section 1: Technical Documentation

The written responses, detailed technical analyses, and conceptual answers for **Section 1** can be found in the standalone text file:
* [`MarcusSpotanski_DataPlatformEngineer_Section1.txt`](Section%201/MarcusSpotanski_DataPlatformEngineer_Section1.txt)
---

## Section 2: Data Pipelines; Terraform and AWS Postgres RDS

This section provisions an AWS RDS PostgreSQL database using Terraform and executes an automated Python ETL pipeline to ingest, standardize, and load sales order records into PostgreSQL.

---

### 1. Prerequisites & Installation

Ensure the following client software packages are installed on your system before proceeding:

* **Python 3.10+**: [Python Downloads](https://www.python.org/downloads/)
* **AWS CLI**: [AWS CLI Installation Guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
* **Terraform CLI**: [Terraform Downloads](https://developer.hashicorp.com/terraform/downloads)

**Windows / PowerShell Users**: If you are configuring AWS CLI or Terraform CLI on Windows for the first time, check out our **[PowerShell Setup Guide](./PowerShell_SetUp_Guide.md)** for step-by-step installation scripts, policy configurations, and troubleshooting tips.

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
$env:PGUSER = "dbadmin"

# Securely prompt for database master password (masked input, PowerShell 5.1 compatible)
$env:PGPASSWORD = [System.Net.NetworkCredential]::new("", (Read-Host -AsSecureString "Enter PostgreSQL Password")).Password

# Execute ETL Pipeline
python pipeline/MarcusSpotanski_DataPlatformEngineer_ETL.py
```

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

## Section 3: ML API; Secure Iris Classifier API with FASTAPI and ngrok

Welcome! This project demonstrates how to build, secure, and deploy a machine learning classification model using a lightweight, modern web framework. It takes physical flower measurements (sepal and petal dimensions) and predicts the specific species of an Iris flower using a Random Forest model.

This guide walks you through the technical decisions made during development, how to set up your environment, and how to use the interactive command-line interface (CLI) to test the model.

---

### 1. Architecture & Technical Decisions

I chose a specific stack and implemented security controls at distinct layers of the application to balance **simplicity, customization, and performance**.

#### Why FastAPI and ngrok?
* **FastAPI:** I selected FastAPI as our core web framework because it is incredibly fast, highly customizable, and designed for building APIs in Python. It automatically generates documentation and integrates seamlessly with data validation tools.
* **ngrok:** While this app runs perfectly locally, the code is designed to automatically detect and integrate with ngrok. ngrok creates a secure tunnel from the public internet directly to your local machine, making it perfect for secure, rapid prototyping and testing without needing to manage complex cloud infrastructure.

#### Why Control Portions Were Added Where They Are
To build a resilient application, I implemented controls as "checkpoints" that incoming data must pass before it ever reaches our machine learning model:

1. **IP Whitelisting (Middleware Level):** Before the server even processes what the request is asking for, middleware checks the visitor's IP address. If the network isn't trusted, the connection is dropped immediately.
2. **Rate Limiting (Endpoint Level):** I set a limit of 5 requests per minute to prevent malicious users from spamming the model and overwhelming the server.
3. **Authentication (Header Level):** A custom API key (`S3_API_KEY`) was required to ensure only authorized users can trigger the model. I enforced this via environment variables rather than hardcoding passwords into the script.
4. **Input Validation (Schema Level):** Using Pydantic, I validated that the user's measurements are numbers between 0 and 15 cm. Catching bad data (like text or negative numbers) *before* it touches the model prevents Python crashes and guarantees the model only processes what it was trained to understand.

---

### 2. Troubleshooting Note: Python Versioning

During development, **Python 3.14 caused compatibility issues** with some of the required machine learning and server libraries.

To ensure this project runs smoothly, **you must use Python 3.12.10**. If you run into installation errors, please download Python 3.12.10, create a fresh virtual environment, and install the updated dependencies listed in the current `requirements.txt`.

---

### 3. Setup & Installation

Follow these steps to configure your local environment.

#### Initialize a Virtual Environment
Using Python 3.12.10, create and activate a new virtual environment to keep your dependencies isolated:

**macOS / Linux:**
```bash
python3.12 -m venv venv
source venv/bin/activate
```

**Windows:**
```cmd
python -m venv venv
venv\Scripts\activate
```

#### Install Dependencies
Install the required packages from the provided `requirements.txt` file:
```bash
pip install -r requirements.txt
```

---

### 4. Running the Application

#### Set Your Secure API Key
Before starting the server or client, you must define your secret API key in your terminal session.

**macOS / Linux:**
```bash
export S3_API_KEY="your-custom-secret-key"
```
**Windows (Command Prompt / PowerShell):**
```cmd
set S3_API_KEY=your-custom-secret-key
```

#### Launch the FastAPI Server
Start the local web server. On its first run, it will automatically download the Iris dataset, train the Random Forest model, and save a `model.pkl` file to your directory.

```bash
uvicorn app:app --port 8000 --reload
```

#### Run the Interactive Client
Open another terminal window (ensure you set your `S3_API_KEY` in this new window as well) and launch the interactive CLI client.

The client will automatically detect your local server. It includes a helpful botanical guide for the measurements and will prompt you step-by-step for your inputs!

```bash
python client.py
```
