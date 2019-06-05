ARG VERSION=8u151

FROM openjdk:${VERSION}-jdk as BUILD

COPY . /src
WORKDIR /src
RUN ./mvn clean package

FROM openjdk:${VERSION}-jre

COPY --from=BUILD /target/transactionsproducer-1.0-SNAPSHOT-jar-with-dependencies.jar /bin/runner/run.jar
WORKDIR /bin/runner

CMD ["java","-jar","run.jar"]
