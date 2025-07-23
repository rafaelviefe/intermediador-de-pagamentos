FROM ghcr.io/graalvm/graalvm-community:21-ol8 AS builder

WORKDIR /app
COPY . .

RUN ./mvnw -Pnative -Dmaven.test.skip=true native:compile

FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y libz-dev && rm -rf /var/lib/lists/*

WORKDIR /app

COPY --from=builder /app/target/pagamentos .

EXPOSE 8080

ENTRYPOINT ["./pagamentos"]
