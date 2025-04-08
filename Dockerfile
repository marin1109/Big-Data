FROM bitnami/spark:3.5.4
USER root

RUN mkdir -p /app && chmod 777 /app
COPY k8s_tests.py /app/k8s_tests.py
COPY datasets/ /app/datasets/
WORKDIR /app
RUN chmod +x /app/k8s_tests.py
