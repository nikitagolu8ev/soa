FROM golang:1.21-alpine

WORKDIR /src
COPY . .

WORKDIR /src/stat_service

RUN go mod download -x

RUN go build .

RUN touch posts.db

ENTRYPOINT ["./stat_service"]