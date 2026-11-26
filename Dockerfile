FROM python:3.11-slim-bookworm@sha256:ef56ae98435cc40d8a27020b0b32a9941515acd5840f0fba34858bb34dd362d1
WORKDIR /work
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless tini ca-certificates && rm -rf /var/lib/apt/lists/*
COPY requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir --require-hashes -r /tmp/requirements.lock
COPY docker/jars.lock /tmp/jars.lock
RUN python -c "import urllib.request,pathlib; [(pathlib.Path('/tmp') / url.split('/')[-1]).write_bytes(urllib.request.urlopen(url).read()) for _,url in (line.split() for line in open('/tmp/jars.lock'))]" && cd /tmp && awk '{n=split($2,a,"/"); print $1 "  " a[n]}' jars.lock | sha256sum -c - && cp *.jar /usr/local/lib/python3.11/site-packages/pyspark/jars/
ENV SPARK_LOCAL_IP=127.0.0.1 MPLBACKEND=Agg PYTHONUNBUFFERED=1 MEDLENS_ROOT=/work
COPY . /work
RUN pip install --no-deps --no-build-isolation -e .
ARG MEDLENS_BUILD_REVISION
ARG MEDLENS_BUILD_STATE
ENV MEDLENS_BUILD_REVISION=${MEDLENS_BUILD_REVISION} MEDLENS_BUILD_STATE=${MEDLENS_BUILD_STATE}
ENTRYPOINT ["tini","--"]
CMD ["jupyter","lab","--ip=0.0.0.0","--allow-root","--no-browser","--ServerApp.root_dir=/work"]
