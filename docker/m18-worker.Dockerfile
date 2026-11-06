FROM magic-glass-box/runtime-proof:m18c-068b6a8

RUN python3 -m venv /opt/mgb-worker-tools \
    && /opt/mgb-worker-tools/bin/pip install --no-cache-dir \
        attrs==26.1.0 \
        build==1.3.0 \
        cffi==2.1.1 \
        coverage==7.16.2 \
        cryptography==50.0.2 \
        jsonschema==4.26.0 \
        pycparser==3.11 \
        PyJWT==2.15.1 \
        referencing==0.37.0 \
        rfc3339-validator==0.1.4 \
        rpds-py==2026.9.1 \
        ruff==0.17.0 \
        setuptools==77.0.1 \
        wheel==0.45.1

ENV PATH="/opt/mgb-worker-tools/bin:${PATH}"
WORKDIR /workspace
