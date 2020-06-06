FROM node:13

RUN apt-get update && apt-get install -y cron && apt-get clean

COPY daily/run_reports /etc/cron.daily/

# start cron as the entrypoint, as long as cron is up then the container is up
CMD [ "cron" "-f" ]
