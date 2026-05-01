FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    git openssh-client \
    && rm -rf /var/lib/apt/lists/*
CMD ["python", "/deploy/server.py"]
