# Base Python Image

FROM python:3.11-slim


# Set Working Directory In the Container

WORKDIR /app

# Install System Dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy Python Dependencies

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Rest Of The Project Files

COPY . .

# Expose Port (If Your App Runs On A Specific Port, For Example 8000)
#EXPOSE 8000 


# Command To Run The Application (Modify As Needed)
CMD ["python3", "src/pipeline.py"]



