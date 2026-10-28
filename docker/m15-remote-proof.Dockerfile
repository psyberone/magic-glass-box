# syntax=docker/dockerfile:1

FROM python:3.12-slim@sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install --yes --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip install --no-cache-dir \
        "jsonschema[format]==4.26.0" \
        "PyJWT[crypto]==2.15.1" \
        "pytest==9.1.1" \
        "rfc3339-validator==0.1.4" \
    && useradd --uid 65532 --create-home \
        --home-dir /home/mgb-proof --shell /usr/sbin/nologin mgb-proof

COPY --chmod=0555 docker/m15_remote_proof.py /usr/local/bin/mgb-m15-proof

LABEL io.magicglassbox.milestone="15" \
      io.magicglassbox.purpose="credential-free remote execution proof"

USER 65532:65532
WORKDIR /workspace
ENTRYPOINT ["/usr/local/bin/mgb-m15-proof"]
CMD ["prove"]
