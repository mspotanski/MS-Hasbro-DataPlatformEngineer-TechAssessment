# Windows & PowerShell Setup Guide: AWS CLI and Terraform

This guide is designed for Windows users setting up **AWS CLI** and **Terraform CLI** for the first time. Standard tool documentation often focuses on macOS/Linux or glosses over Windows-specific setup steps. This document walks you through the complete installation, environment configuration, AWS IAM setup, and credential management using PowerShell.

---

## 1. Downloading and Installing the Tools

This implementation relies on **Amazon Web Services (AWS)** for cloud hosting and **HashiCorp Terraform** for managing infrastructure as code. You will need active accounts for both before proceeding.

### Step A: Install AWS CLI
1. Download the official Windows 64-bit installer from the [AWS CLI Installation Guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
2. Run the `.msi` setup file and follow the standard on-screen installation prompts.
3. Once completed, open a **PowerShell** terminal window and run the following command to verify it installed correctly:
   ```powershell
   aws --version
   ```

---

### Step B: Install Terraform CLI (Windows Environment Path Configuration)
Linux and macOS users can install Terraform via simple terminal commands, but Windows requires downloading the executable binary and manually telling Windows where to find it (adding it to your System Environment Path).

1. Go to the [Terraform Downloads Page](https://developer.hashicorp.com/terraform/downloads).
2. Under the Windows section, choose the **AMD64** download link (this satisfies almost all modern 64-bit Windows computers).
3. Open your `Downloads` folder, locate the downloaded `.zip` file, and extract its contents into a permanent location on your computer. 
   > *Recommended Location:* Extract it into `C:\Terraform\` (you can create a folder named `Terraform` directly inside your `C:` drive).
4. **Add Terraform to your Windows Environment Path:**
   * Open the Windows Search bar, type `sysdm.cpl`, and press **Enter**. This opens the **System Properties** window.
   * Click on the **Advanced** tab at the top.
   * Click the **Environment Variables...** button near the bottom. A new window will pop up showing *User variables* and *System variables*.
   * Under **System variables** (if you are on a shared PC, choose *User variables* instead), scroll to find the variable named `Path`, select it, and click **Edit...**.
   * Click **New** on the right side of the editor window.
   * Either paste the exact file path where your `terraform.exe` is saved (e.g., `C:\Terraform`) or click **Browse...** to navigate to and select the folder.
   * Click **OK** on all open windows to save your changes and exit System Properties.
5. **Restart PowerShell:** Close any open PowerShell windows and launch a fresh PowerShell window. Test the installation by running:
   ```powershell
   terraform --version
   ```

---

## 2. Setting Up AWS IAM Credentials & Access Keys

To allow Terraform to build cloud resources automatically on your behalf, we need to create a dedicated administrative user inside AWS and grant it specific permissions.

> **Note on Permissions:** In a production enterprise environment, permissions are typically assigned to User Groups rather than individual users. For this technical assessment, we will assign permissions directly to a single dedicated user to grant Terraform the required rights to create, modify, and delete resources.

### Step A: Create the IAM User in AWS
1. Sign in to your **AWS Management Console** in a web browser.
2. In the top search bar, type `IAM` and select the result labeled **IAM** (Identity and Access Management).
3. On the left sidebar, under **Access Management**, click **Users**.
4. Click the **Create user** button.
5. Enter a clear User Name so you know what it is used for (e.g., `ms-terraform`).
6. **Do NOT check** the box for *"Provide user access to the AWS Management Console"*. We only need this identity for local command-line scripts, not browser logins. Click **Next**.

### Step B: Assign Permissions Policies
1. In the **Permissions options** section, select **Attach policies directly**.
2. In the **Permissions policies** search box, search for and check the boxes next to each of the following **4 required policies**:

| Policy Name | Why It Is Needed |
| :--- | :--- |
| `IAMReadOnlyAccess` | Allows local scripts and CLI tools to check what permissions they have without being able to edit or grant new permissions[cite: 6]. |
| `AmazonRDSFullAccess` | Allows Terraform to provision, update, and remove our PostgreSQL relational database instance (`aws_db_instance`)[cite: 3, 6]. |
| `AmazonS3FullAccess` | Allows Terraform to create, manage, and delete the S3 storage bucket used for storing raw sales data (`aws_s3_bucket`)[cite: 3, 6]. |
| `AmazonVPCFullAccess` | Allows Terraform to create and manage basic network infrastructure, like Virtual Private Clouds (VPCs) and Security Groups (`aws_security_group`) that open database port `5432`[cite: 3, 6]. |

3. Leave the *Set permissions boundary* section blank and click **Next**.
4. *(Optional)* Add a tag if you wish (e.g., Key: `purpose`, Value: `terraform`)[cite: 6].
5. Review the user summary and click **Create user**[cite: 6].

### Step C: Generate Your Access Key and Secret Key
1. After user creation completes, click on your new user's name (`ms-terraform`) in the user list to open its profile[cite: 6].
2. Click on the **Security credentials** tab.
3. Scroll down to the *Access keys* section and click **Create access key**[cite: 6].
4. Select **Command Line Interface (CLI)** as your primary use case, check the confirmation checkbox at the bottom, and click **Next**.
5. Click **Create access key**.
6. **CRITICAL STEP:** AWS will display your **Access Key** and **Secret Access Key**. 
   * Copy both keys to a secure location, or click **Download .csv file**. 
   * *The Secret Access Key is shown only once and cannot be retrieved later if lost.*

---

## 3. Configuring Credentials in the AWS CLI

Now that your credentials exist in AWS, you must configure your local PowerShell terminal to use them.

1. Open PowerShell and run:
   ```powershell
   aws configure
   ```
2. Enter your credentials when prompted step-by-step:
   * **AWS Access Key ID:** Paste your Access Key ID from Step 2C.
   * **AWS Secret Access Key:** Paste your Secret Access Key from Step 2C.
   * **Default region name:** Enter `us-east-1` *(this matches the default region configured in `variables.tf`)*[cite: 5].
   * **Default output format:** Enter `json`.
3. Verify your connection by asking AWS to confirm your active user identity:
   ```powershell
   aws sts get-caller-identity
   ```
   *If configured properly, AWS will return a JSON block displaying your Account ID and User ARN.*

---

## 4. Understanding the Project's Terraform Variables

Before executing any commands inside Terraform, here is how our local code links to AWS:

* **AWS Region (`aws_region`):** Defined in `variables.tf`, this defaults to `us-east-1`[cite: 5]. All database and storage resources will be built inside this data center region[cite: 3, 5].
* **Database Password (`db_password`):** Defined as a sensitive variable in `variables.tf`[cite: 5]. Because passwords should never be saved in clear text within code files, Terraform will securely prompt you to enter a password when running deployment scripts.
* **S3 Bucket Naming Constraint (`main.tf`):** AWS requires **S3 bucket names to be globally unique across all AWS users in the world**. 
  ```hcl
  resource "aws_s3_bucket" "csv_storage" {
    bucket        = "${var.project_name}-bucket-182484"
    force_destroy = true
  }
  ```
  If `terraform apply` fails stating that the bucket name already exists, edit the number `182484` in `infra/main.tf` to any custom string of numbers or letters to make it unique again[cite: 3].

---

## 5. Summary Execution Workflow

With AWS CLI and Terraform configured, you can execute the pipeline setup from PowerShell:

```powershell
# 1. Navigate to the infrastructure folder
cd infra

# 2. Download required modules and AWS provider plugins
terraform init

# 3. Preview what resources AWS will create
terraform plan

# 4. Build the infrastructure (requires entering your database password when prompted)
terraform apply
```
