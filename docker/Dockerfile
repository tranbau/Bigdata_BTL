FROM bitnami/spark:latest

# Set the working directory
WORKDIR /usr/src/app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set other configurations or copy your application files if needed
# ...

# Start Spark or define the command to run your application
# ...
