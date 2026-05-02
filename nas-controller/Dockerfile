FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg curl git openssh-client && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.5.1.tgz \
    | tar xz --strip-components=1 -C /usr/local/bin docker/docker
RUN pip install flask yt-dlp mutagen --no-cache-dir
WORKDIR /app
COPY app.py .
CMD ["sh", "-c", "printf 'nameserver 8.8.8.8\nnameserver 1.1.1.1\n' > /etc/resolv.conf && exec python /DATA/AppData/nas-controller/app.py"]
