# Use the official Python image as the base
FROM python:3.10.5-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file to the container
COPY requirements.txt ./

# Install the dependencies
RUN pip install -r requirements.txt

# Copy the application code to the container
COPY . .

# Expose the port that the application will listen on (adjust if needed)
EXPOSE 8080

# Run the application when the container starts
CMD ["python", "main.py"]
