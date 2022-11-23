CREATE UNLOGGED TABLE issuance (
  name varchar(255) PRIMARY KEY NOT NULL,
  issuances bytea NOT NULL
) WITH (autovacuum_enabled=false);
