# syntax=docker/dockerfile:1

FROM node:22-slim@sha256:83f487e0a63425e5b4d146fb5e5be574bcbe1b7b843d3ebafdd95eaf7767a7e5

ARG CODEX_VERSION=0.161.0
ARG CLAUDE_CODE_VERSION=2.1.296

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates python3 python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN npm install --global --omit=dev \
        "@openai/codex@${CODEX_VERSION}" \
        "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}" \
    && npm cache clean --force

WORKDIR /opt/magic-glass-box-source
COPY pyproject.toml README.md ./
COPY src ./src
RUN python3 -m venv /opt/magic-glass-box \
    && /opt/magic-glass-box/bin/pip install --no-cache-dir --no-deps . \
    && ln -s /opt/magic-glass-box/bin/magic-glass-box-worker \
        /usr/local/bin/magic-glass-box-worker \
    && rm -rf /opt/magic-glass-box-source

LABEL io.magicglassbox.runtime-proof.codex-version="${CODEX_VERSION}" \
      io.magicglassbox.runtime-proof.claude-code-version="${CLAUDE_CODE_VERSION}"

WORKDIR /workspace
