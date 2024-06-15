FROM golang:1.21-alpine

WORKDIR /src
COPY . .

WORKDIR /src/main_service

RUN go mod download -x

RUN go build .

ENTRYPOINT ["./main_service", "--private_key_path", "private_unencrypted.pem", "--public_key_path", "public.pem"]