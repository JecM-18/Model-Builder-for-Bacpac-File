FROM python:3.11-slim

WORKDIR /app

# Install SqlPackage dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    libicu-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Download and install SqlPackage
RUN curl -L https://aka.ms/sqlpackage-linux -o sqlpackage.zip \
    && unzip sqlpackage.zip -d /opt/sqlpackage \
    && chmod +x /opt/sqlpackage/sqlpackage \
    && ln -s /opt/sqlpackage/sqlpackage /usr/local/bin/SqlPackage \
    && rm sqlpackage.zip

# Copy the script
COPY compare_models.py .

# Create directory for XML files
RUN mkdir -p /app/data

# Set default command
CMD ["python", "compare_models.py"]
