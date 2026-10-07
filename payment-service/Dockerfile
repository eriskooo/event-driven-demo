# syntax=docker/dockerfile:1

# --- build: Maven cache v BuildKit mountu zrychlí opakované buildy ---
FROM maven:3.9-eclipse-temurin-21-noble AS build
WORKDIR /workspace
COPY pom.xml .
COPY src src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q package -DskipTests

# --- extract: rozbalení do vrstev, aby se při změně kódu nepřenášely závislosti ---
FROM eclipse-temurin:21-jre-noble AS extract
WORKDIR /extract
COPY --from=build /workspace/target/app.jar app.jar
RUN java -Djarmode=tools -jar app.jar extract --layers --launcher --destination layers

# --- runtime ---
FROM eclipse-temurin:21-jre-noble
WORKDIR /app
RUN useradd --system --uid 10001 --no-create-home spring
COPY --from=extract /extract/layers/dependencies/ ./
COPY --from=extract /extract/layers/spring-boot-loader/ ./
COPY --from=extract /extract/layers/snapshot-dependencies/ ./
COPY --from=extract /extract/layers/application/ ./
USER 10001
EXPOSE 8080
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
