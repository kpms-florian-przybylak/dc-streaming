# Use an official Python image as a base
FROM python:3.9-buster

LABEL authors="floprz"

# Set the working directory
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt /app
# Install additional dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed Python packages (if you need this in addition to requirements.txt)
RUN pip install --no-cache-dir opcua

# Run opc_client.py when the container launches
# Replace opc_client.py with the name of your main file if different
CMD ["python", "./main.py"]