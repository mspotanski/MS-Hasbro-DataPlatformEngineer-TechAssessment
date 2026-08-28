# Windows & PowerShell Setup Guide: AWS CLI and Terraform
--
This guide provides step-by-step instructions for installing, configuring, and verifying the **AWS CLI** and **Terraform CLI** on Windows using PowerShell. 
---

## 1. Prerequisites & Tool Installation

### Step A: Install AWS CLI
1. Download the official 64-bit Windows installer from the [AWS CLI Installation Guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
2. Run the `.msi` file and follow the standard installation wizard prompts.
3. Open a new PowerShell terminal and verify the installation:
```powershell
   aws --version
```
### Step B: Install Terraform CLI (Windows Environment Path Setup)Unlike Linux or macOS, Windows requires manually extracting the executable and registering it in your System Path.Download the AMD64 Windows zip package from Terraform Downloads.Extract the downloaded .zip file into a permanent local directory.Example: C:\Terraform\Add the Terraform folder to your Windows System Environment Path:Press Win + R, type sysdm.cpl, and hit Enter to open System Properties.Navigate to the Advanced tab and click Environment Variables.Under System Variables (or User Variables if you prefer non-administrative access), select the variable named Path and click Edit.Click New, paste your directory path (e.g., C:\Terraform\), and click OK.Click OK on all remaining dialog boxes to save changes.Restart any active PowerShell windows, then verify the installation:PowerShellterraform --version
## 2. AWS IAM User & Access Key ProvisioningTerraform requires AWS credentials with sufficient permissions to manage database instances, storage buckets, and networking components.Step A: Create the IAM UserSign in to the AWS Management Console and navigate to IAM (Identity and Access Management).On the left sidebar under Access Management, click Users $\rightarrow$ Create User.Set the User Name (e.g., ms-terraform).Leave the console access box unchecked (this user is dedicated to CLI execution only). Click Next.Step B: Attach Required PoliciesSelect Attach policies directly and attach the following four permissions policies:Policy NameTechnical RationaleIAMReadOnlyAccessAllows CLI user identity verification (aws sts get-caller-identity).AmazonRDSFullAccessGrants permissions to provision, modify, and destroy PostgreSQL RDS instances.AmazonS3FullAccessGrants permissions to create, update, and drop S3 storage buckets.AmazonVPCFullAccessGrants permissions to manage Virtual Private Cloud (VPC) subnets and security groups.Click Next, review the configuration, and click Create User.Step C: Generate Security CredentialsSelect your newly created user from the IAM Users list.Navigate to the Security Credentials tab and click Create Access Key.Select Command Line Interface (CLI) as the use case, check the acknowledgment box, and click Next.(Optional) Add a tag description (e.g., terraform-cli-key).Click Create Access Key.IMPORTANT: Copy your Access Key ID and Secret Access Key immediately, or click Download .csv file.Note: The Secret Access Key will never be shown again after you leave this screen.3. Local AWS CLI Credentials ConfigurationNow that your credentials exist in AWS, map them into your local Windows shell environment.Open PowerShell and run:PowerShellaws configure
Input your values when prompted:AWS Access Key ID: Paste your Access Key ID from Step 2C.AWS Secret Access Key: Paste your Secret Access Key from Step 2C.Default region name: us-east-1 (matches default region in variables.tf).Default output format: jsonVerify that your CLI authentication works correctly:PowerShellaws sts get-caller-identity
If successful, PowerShell will display your AWS Account ID, User ARN, and User ID.4. Terraform Code Variables & Configuration AlignmentBefore provisioning infrastructure, ensure your local execution context matches the expected variable definitions in the project code.Expected Infrastructure VariablesThe Terraform configuration relies on specific environment parameters defined in variables.tf and main.tf:aws_region: Defaults to us-east-1. If you changed your default AWS region during aws configure, update var.aws_region or specify it via command line.project_name: Defaults to ms-hasbro-dpe-interview. This prefix is applied dynamically to S3 buckets and security groups.db_password: Marked as a sensitive variable in variables.tf. Terraform will require a password input during execution or expect it to be supplied securely.S3 Bucket Naming ConstraintIn main.tf, the S3 bucket is named dynamically:Terraformbucket = "${var.project_name}-bucket-182484"
AWS S3 bucket names must be globally unique across all AWS accounts worldwide. If deployment fails with a BucketAlreadyExists error, update the numerical suffix in main.tf to a custom unique string.5. Execution Workflow VerificationOnce AWS CLI and Terraform are verified, navigate to the infra/ folder in PowerShell to run your deployment:PowerShell# Navigate into the Terraform workspace
cd infra

# Initialize HashiCorp AWS provider (~> 5.0)
terraform init

# Preview resource creation (RDS PostgreSQL 15, Security Groups, S3)
terraform plan

# Provision resources to AWS
terraform apply
