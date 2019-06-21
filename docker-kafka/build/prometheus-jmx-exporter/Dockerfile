FROM java:8

RUN mkdir /opt/jmx_prometheus_httpserver && wget 'http://central.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_httpserver/0.1.0/jmx_prometheus_httpserver-0.1.0-jar-with-dependencies.jar' -O /opt/jmx_prometheus_httpserver/jmx_prometheus_httpserver.jar

ADD https://github.com/kelseyhightower/confd/releases/download/v0.15.0/confd-0.15.0-linux-amd64 /usr/local/bin/confd
COPY confd /etc/confd
RUN chmod +x /usr/local/bin/confd

COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
