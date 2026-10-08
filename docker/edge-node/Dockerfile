# A small Linux "edge node": the kind of server you reach with PuTTY at work.
# It has Python, the AWS CLI, git and an SSH server. Code is deployed with git clone.
FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends openssh-server cron git vim nano less procps curl ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /run/sshd \
 && ssh-keygen -A

# The service account that runs ingestion jobs (change the password after first login: passwd)
RUN useradd -m -s /bin/bash etluser && echo 'etluser:etl123' | chpasswd

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt awscli

COPY docker/edge-node/motd /etc/motd
ENV TZ=UTC
EXPOSE 22
# Start cron (for the scheduling exercise) and the SSH server
CMD ["/bin/sh", "-c", "service cron start && exec /usr/sbin/sshd -D -e"]
