FROM confluentinc/cp-base:5.2.0

COPY target/streams-boot.jar /streams-boot.jar

RUN chmod +x /streams-boot.jar

ENTRYPOINT ["java", "-jar", "/streams-boot.jar"]
