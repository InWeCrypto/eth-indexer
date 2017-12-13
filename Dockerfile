FROM golang:1.9.2-stretch

LABEL maintainer="support@inwecrypto.com"

COPY . /go/src/github.com/inwecrypto/eth-indexer

RUN go install github.com/inwecrypto/eth-indexer/cmd/eth-indexer && rm -rf /go/src

VOLUME ["/etc/inwecrypto/indexer/eth"]

WORKDIR /etc/inwecrypto/indexer/eth

CMD ["/go/bin/eth-indexer","--conf","/etc/inwecrypto/indexer/eth/indexer.json"]