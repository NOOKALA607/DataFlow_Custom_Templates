# Use a stable Apache Beam Python 3.11 SDK image
FROM apache/beam_python3.11_sdk:latest

# Set working directory
WORKDIR /pipeline

# Copy your pipeline script
COPY Transformation_to_bigquery.py .

# (Optional) Copy requirements if you have dependencies
# COPY requirements.txt .
# RUN pip install -r requirements.txt

# Define the entrypoint for the pipeline
ENTRYPOINT ["python", "Transformation_to_bigquery.py"]
