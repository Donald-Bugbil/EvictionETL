FROM quay.io/astronomer/astro-runtime:13.0.0
RUN pip install boto3
RUN pip install pyarrow
