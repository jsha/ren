```
export POSTGRES_PASSWORD=$(openssl rand -hex 20)
docker run --name some-postgres -e POSTGRES_PASSWORD -d postgres
export DSN="postgres://postgres:$POSTGRES_PASSWORD@some-postgres"
```
