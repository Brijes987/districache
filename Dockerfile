# Builds the cache and runs one server. No dependencies at run time, so the final image is a plain JRE plus the classes.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/classes /app/classes
# Replicated server by default; docker-compose.yml passes the options.
ENTRYPOINT ["java", "-cp", "/app/classes"]
CMD ["com.distri.cache.node.ReplicatedServerMain"]
