FROM debian:12-slim as builder

FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=builder /lib/x86_64-linux-gnu/libz.so.1 /lib/x86_64-linux-gnu/libz.so.1

COPY target/pagamentos /app/pagamentos

EXPOSE 8080

CMD ["/app/pagamentos"]
