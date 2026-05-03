FROM ubuntu:22.04

# Install Java and Python
RUN apt-get update && apt-get install -y openjdk-11-jdk python3 python3-pip wget

# Download and Extract Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
RUN tar -xzf spark-3.5.0-bin-hadoop3.tgz && mv spark-3.5.0-bin-hadoop3 /opt/spark

# Set Environment Variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Start JupyterLab by default for the master node
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]