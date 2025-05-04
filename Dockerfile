# Use the official Python base image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /consumer

# Copy the requirements file to the working directory
COPY requirements.txt .

# Install the Python dependencies
RUN pip install -r requirements.txt

# Copy the application code to the working directory
COPY . .

# Expose the port on which the application will run
EXPOSE 8080

# Run the consumer
CMD ["python", "deleteFile.py"]