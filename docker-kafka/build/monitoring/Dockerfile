FROM alpine:latest

RUN apk add --no-cache curl

RUN mkdir -p /dashboards

COPY entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
