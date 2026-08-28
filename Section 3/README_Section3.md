# Secure Iris Classifier API

Welcome! This project demonstrates how to build, secure, and deploy a machine learning classification model using a lightweight, modern web framework. It takes physical flower measurements (sepal and petal dimensions) and predicts the specific species of an Iris flower using a Random Forest model.

This guide walks you through the technical decisions made during development, how to set up your environment, and how to use the interactive command-line interface (CLI) to test the model.

---

## Architecture & Technical Decisions

We chose a specific stack and implemented security controls at distinct layers of the application to balance **simplicity, customization, and performance**.

### Why FastAPI and ngrok?
* **FastAPI:** We selected FastAPI as our core web framework because it is incredibly fast, highly customizable, and designed for building APIs in Python. It automatically generates documentation and integrates seamlessly with data validation tools.
* **ngrok:** While this app runs perfectly locally, the code is designed to automatically detect and integrate with ngrok. ngrok creates a secure tunnel from the public internet directly to your local machine, making it perfect for secure, rapid prototyping and testing without needing to manage complex cloud infrastructure. 

### Why Control Portions Were Added Where They Are
To build a resilient application, we implemented controls as "checkpoints" that incoming data must pass before it ever reaches our machine learning model:

1. **IP Whitelisting (Middleware Level):** Before the server even processes what the request is asking for, middleware checks the visitor's IP address. If the network isn't trusted, the connection is dropped immediately.
2. **Rate Limiting (Endpoint Level):** We added a limit of 5 requests per minute to prevent malicious users from spamming the model and overwhelming the server.
3. **Authentication (Header Level):** We require a custom API key (`S3_API_KEY`) to ensure only authorized users can trigger the model. We enforce this via environment variables rather than hardcoding passwords into the script.
4. **Input Validation (Schema Level):** Using Pydantic, we validate that the user's measurements are numbers between 0 and 15 cm. Catching bad data (like text or negative numbers) *before* it touches the model prevents Python crashes and guarantees the model only processes what it was trained to understand.

---

## Troubleshooting Note: Python Versioning

During development, **Python 3.14 caused compatibility issues** with some of the required machine learning and server libraries. 

To ensure this project runs smoothly, **you must use Python 3.12.10**. If you run into installation errors, please download Python 3.12.10, create a fresh virtual environment, and install the updated dependencies listed in the current `requirements.txt`.

---

## Setup & Installation

Follow these steps to configure your local environment.

### 1. Initialize a Virtual Environment
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

### 2. Install Dependencies
Install the required packages from the provided `requirements.txt` file:
```bash
pip install -r requirements.txt
```

---

## Running the Application

### 1. Set Your Secure API Key
Before starting the server or client, you must define your secret API key in your terminal session.

**macOS / Linux:**
```bash
export S3_API_KEY="your-custom-secret-key"
```
**Windows (Command Prompt / PowerShell):**
```cmd
set S3_API_KEY=your-custom-secret-key
```

### 2. Launch the FastAPI Server
Start the local web server. On its first run, it will automatically download the Iris dataset, train the Random Forest model, and save a `model.pkl` file to your directory.

```bash
uvicorn app:app --port 8000 --reload
```

### 3. Run the Interactive Client
Open another terminal window (ensure you set your `S3_API_KEY` in this new window as well) and launch the interactive CLI client. 

The client will automatically detect your local server. It includes a helpful botanical guide for the measurements and will prompt you step-by-step for your inputs!

```bash
python client.py
```