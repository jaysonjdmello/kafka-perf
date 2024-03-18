# Use an official Ubuntu base image
FROM ubuntu:latest

# Set the working directory inside the container
WORKDIR /code

# Install required packages
# Update package lists, install build tools, glib and g++ development libraries, and cleanup in one command to keep the image size down
RUN apt-get update && \
    apt-get install -y build-essential libglib2.0-dev librdkafka-dev g++ && \
    rm -rf /var/lib/apt/lists/*

# Command to keep the container running (for demonstration purposes)
CMD tail -f /dev/null
