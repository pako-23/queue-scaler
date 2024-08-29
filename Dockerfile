FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY . .

RUN apk add --update make
RUN make tidy
RUN make

FROM scratch AS approximator

COPY --from=builder /app/approximator /approximator

ENTRYPOINT ["/approximator"]

FROM scratch AS autoscaler

COPY --from=builder /app/autoscaler /autoscaler

ENTRYPOINT ["/autoscaler"]
