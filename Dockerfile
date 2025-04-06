FROM bitnami/spark:3.5.4
USER root
RUN pip install --no-cache-dir pyspark==3.5.4
RUN mkdir -p /app && chmod 777 /app
COPY kubernetes_tests.py /app/kubernetes_tests.py
COPY datasets/ /app/datasets/
WORKDIR /app
RUN chmod +x /app/kubernetes_tests.py
