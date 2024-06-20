FROM golang:1.21-alpine

WORKDIR /src
COPY . .

WORKDIR /src/post_service

RUN go mod download -x

RUN go build .

ENTRYPOINT ["./post_service"]