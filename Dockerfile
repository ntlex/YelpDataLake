ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.10.6

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

ADD ./ /app
WORKDIR /app

ARG PYSPARK_VERSION=3.3.1
RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}
RUN chmod -R pipeline.sh

ENTRYPOINT ["/bin/bash"]
