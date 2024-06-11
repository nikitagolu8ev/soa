FROM golang:1.22-alpine

WORKDIR /main_service
COPY go.* .

RUN go mod download -x

COPY * .

RUN go build .

ENTRYPOINT ["./main", "--private_key_path", "private_unencrypted.pem", "--public_key_path", "public.pem"]