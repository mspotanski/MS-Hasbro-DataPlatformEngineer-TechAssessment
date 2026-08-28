"""
IRIS CLASSIFIER API SERVER
This script sets up a secure, rate-limited, and input-validated web API 
to serve predictions from a scikit-learn machine learning model.
"""

import os
import joblib
import numpy as np
from fastapi import FastAPI, HTTPException, Security, Request, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Set configuration for Rate Limiting
# Define a rule to limit how often a user can request information from 
# the endpoint, preventing floods of requests from a single location (DDoS).
limiter = Limiter(key_func=get_remote_address)

# Initialize FASTAPI object
app = FastAPI(title="Secure Iris Classifier API")

# set the rate limiter to our previously defined one
app.state.limiter = limiter

# Add exception handler to alert user when too many requests are sent within time limit
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Step 2: Define logic for limiting IP addresses
# Screen incoming visitors by their network address. Leaving this blank 
# allows all public traffic, but populating it with specific IP addresses 
# blocks any unauthorized visitor before they reach the model.
# In an actual production environment, we can add in our subnet requirements here, but
# for the current exercise, we can leave blank for usability.
ALLOWED_IPS = []

# define middleware to set request type to http
# HTTPS will automatically be used if you go over public web, but this is preferred for
# local calls. 
@app.middleware("http")
async def restrict_ips(request: Request, call_next):
    # check to see if we have any defined IP address restrictions
    if len(ALLOWED_IPS) > 0:
        # Grab request header to screen incoming request for source IP
        forwarded_for = request.headers.get("x-forwarded-for")
        client_ip = forwarded_for.split(",")[0] if forwarded_for else request.client.host

        # If scraped IP address does not match pattern previously defined, we reject the request
        if client_ip not in ALLOWED_IPS:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, 
                detail="Access denied: IP address not authorized."
            )

    # If no IP restrictions are listed, we allow the request to go through
    response = await call_next(request)
    return response

# Step 3: Automated model training and persistence. 
# Check if the saved machine learning model file exists on disk. If it 
# does not exist, automatically download the built-in iris dataset from sklearn,
# train the classification model, and save it for future use.
MODEL_FILE = "model.pkl"
if not os.path.exists(MODEL_FILE):
    # Load Iris data set from sklearn
    iris = load_iris()

    # Define Random Forest model for dataset
    clf = RandomForestClassifier(n_estimators=100, random_state=182488)

    # Fit Iris dataset onto the random forest
    clf.fit(iris.data, iris.target)

    # Write model contents to the MODEL_FILE variable to live on the disk
    joblib.dump(clf, MODEL_FILE)

# If model.pkl already exists on disk, simply load it into memory
model = joblib.load(MODEL_FILE)

# Step 4: API Key Authentication
# Require visitors to provide a secret password (API key) via their request 
# headers. This ensures only authorized users can trigger model predictions.
# Obtain environmental variable S3_API_KEY. It is expected that before setting up
# the server and running the client.py file, that this value is defined in the console
# If it is not, then the client.py file will not be able to get requests to app
API_KEY = os.getenv("S3_API_KEY")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key(key: str = Security(api_key_header)):
    """
    Compares the secret key provided by the user against the server's 
    environment variable. Rejects the request if they do not match.
    """
    if not API_KEY or key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key"
        )
    return key

# Step 5: Limit Input formatting and type
# Establish strict rules for the data users send. Using an ellipsis (...) 
# marks fields as strictly required, and numerical bounds prevent corrupt 
# or unrealistic numbers from breaking the model.
# These are the values that will be passed to the model to obtain the
# inference of what kind of Iris flower it is.
class IrisInput(BaseModel):
    sepal_length: float = Field(..., gt=0.0, lt=15.0, description="Sepal length in cm")
    sepal_width: float = Field(..., gt=0.0, lt=15.0, description="Sepal width in cm")
    petal_length: float = Field(..., gt=0.0, lt=15.0, description="Petal length in cm")
    petal_width: float = Field(..., gt=0.0, lt=15.0, description="Petal width in cm")

# Step 6: Prediction Endpoint
# Create the public-facing URL path where users send their flower measurements 
# to receive a machine learning prediction, protected by authentication and rate limits.
@app.post("/predict", dependencies=[Security(verify_api_key)])
@limiter.limit("5/minute")
def predict(request: Request, payload: IrisInput):
    """
    Takes validated user measurements, feeds them into the random forest model, 
    and returns the predicted iris species name along with confidence scores.
    """
    # Define dictionary to store user inputs for model inference
    data = np.array([[
        payload.sepal_length, 
        payload.sepal_width,
        payload.petal_length, 
        payload.petal_width
    ]])

    # Define classes from Iris dataset that it can predict on
    classes = ["setosa", "versicolor", "virginica"]

    # Get predicted Iris flower from inference
    pred = int(model.predict(data)[0])

    # Obtain model confidence for each class
    probabilities = model.predict_proba(data)[0].tolist()
    
    return {
        "class_id": pred,
        "class_name": classes[pred],
        "confidence_score": probabilities[pred],
        "all_probabilities": dict(zip(classes, probabilities))
    }