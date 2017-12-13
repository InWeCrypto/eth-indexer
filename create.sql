-- CREATE DATABASE eth;

CREATE TABLE ETH_TX(
  "id"          SERIAL PRIMARY KEY,
  "tx"          VARCHAR(128) NOT NULL, -- tx id
  "from"        VARCHAR(128) NOT NULL, -- value out address
  "to"          VARCHAR(128) NOT NULL, -- value out address
  "createTime"  TIMESTAMP    NOT NULL DEFAULT NOW()
);