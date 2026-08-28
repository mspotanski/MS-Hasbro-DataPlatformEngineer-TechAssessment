"""
IRIS CLASSIFIER COMMAND-LINE CLIENT
This script provides a user-friendly terminal interface for non-technical users 
to input flower measurements, automatically discovers the active server endpoint, 
and displays the machine learning prediction results.
"""

import os
import sys
import requests

# Step 1: Automatic Endpoint Discovery
# Programmatically check if an ngrok tunneling tool is running locally 
# to discover the active public URL, falling back gracefully to local testing.
def get_active_api_url():
    """
    Inspects local diagnostic ports to find the active web address 
    without requiring the user to manually copy and paste long URLs.
    """
    # If the user has laready defined the API URL, obtain it here and proceed
    if os.getenv("API_URL"):
        return os.getenv("API_URL")

    # Using ngrok tunnel, scrape the public URL value used for the locally running server
    # This avoids need to hard-code URL or require user to pass it themselves as variable
    try:
        response = requests.get("http://localhost:4040/api/tunnels", timeout=2)
        
        # check to see if response was successful
        if response.status_code == 200:
            # Identify Tunnels key from response JSON
            tunnels = response.json().get("tunnels", [])

            # For each tunnel value, identify the https URLs, and record the first public URL and return
            for tunnel in tunnels:
                if tunnel.get("proto") == "https":
                    public_url = tunnel.get("public_url")
                    return f"{public_url}/predict"
    except requests.exceptions.RequestException:
        pass

    # If no public URL value is found, direct API to localhost traffic
    return "http://localhost:8000/predict"

# Initialize API_URL value
API_URL = get_active_api_url()

# Initialize API Key form environment
# NOTE: it is expected that the end user defines the environmental variable of
# S3_API_KEY BEFORE running both the server and the client. If this is not defined
# beforehand, than the client will not be able to get any requests through to the server
API_KEY = os.getenv("S3_API_KEY")

# Step 2: Secuirty Credential Verification
# Ensure that the secret API key environment variable has been set 
# before attempting to communicate with the secure server.
# =====================================================================
def validate_environment():
    """
    Verifies that the user has provided their secret password key 
    in their terminal session before executing requests.
    """
    if not API_KEY:
        print("Error: The 'S3_API_KEY' environment variable is not set.")
        print("Please set it in your terminal before running the script:")
        print("  export S3_API_KEY='your-secret-key'")
        sys.exit(1)

# STEP 3: Interactive Input Collection & Validation
# Prompt the user step-by-step for flower measurements, ensuring they 
# type valid positive numbers within a realistic range.
def get_float_input(prompt_text):
    """
    Repeatedly prompts the user for a numerical value until 
    a valid positive number is successfully provided.
    """
    while True:
        try:
            val = float(input(prompt_text))
            if val <= 0 or val >= 15:
                print("Value must be greater than 0 and less than 15.")
                continue
            return val
        except ValueError:
            print("Invalid input. Please enter a numerical value.")

# Step 4: Execution and Response formatting.
# Package the collected inputs into a structured JSON payload, attach 
# the security headers, send the request, and print clear results.
def main():
    # First, validate API key is defined and present
    validate_environment()

    # Begin main execution
    print("==========================================")
    print("      IRIS CLASSIFIER CLI CLIENT          ")
    print("==========================================")
    print(f"Target Endpoint: {API_URL}\n")

    # Add descriptions of model inputs for non-flower-friendly users
    # Botanical definitions to guide non-technical users
    print("FLOWER MEASUREMENT GUIDE:")
    print("  - Sepal: The outer protective parts that enclose the petals before blooming.")
    print("  - Petal: The inner, brightly colored parts of the flower.")
    print("-" * 42)
    print("Please enter the iris flower measurements (in cm):\n")

    # Capture user input for model inference
    sepal_length = get_float_input("Enter Sepal Length (e.g., 5.1): ")
    sepal_width  = get_float_input("Enter Sepal Width  (e.g., 3.5): ")
    petal_length = get_float_input("Enter Petal Length (e.g., 1.4): ")
    petal_width  = get_float_input("Enter Petal Width  (e.g., 0.2): ")

    # Define dictionary to hold user input to send in JSON format in API Call
    payload = {
        "sepal_length": sepal_length,
        "sepal_width": sepal_width,
        "petal_length": petal_length,
        "petal_width": petal_width
    }

    # Set headers with required API key and description
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": API_KEY
    }

    print("\nSending request to API...")
    
    try:
        # Send request to server
        response = requests.post(API_URL, json=payload, headers=headers)

        # Check if request is successful
        if response.status_code == 200:
            # Store result as JSON object and extract values from key
            result = response.json()
            print("\n------------------------------------------")
            print(" PREDICTION SUCCESSFUL")
            print(f"    Species:    {result['class_name'].upper()}")
            print(f"    Confidence: {result['confidence_score'] * 100:.2f}%")
            print("------------------------------------------")
        elif response.status_code == 401:
            print("Authentication Error: Invalid or missing API Key.")
        elif response.status_code == 429:
            print("Rate Limit Exceeded: Too many requests. Please wait a moment.")
        else:
            print(f"Error [{response.status_code}]: {response.text}")

    except requests.exceptions.ConnectionError:
        print(f"Connection Failed: Could not reach {API_URL}. Check if your server or ngrok tunnel is running.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()