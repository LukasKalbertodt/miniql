# miniql

Prepare Postgres DB via Docker:

```
docker run --name mini-pg -p 5555:5432 -e POSTGRES_PASSWORD=test -d postgres
```

(afterwards you can just `docker start mini-pg`)

Create a database called `minitest` and apply the `minidb-backup.sql`.

Finally, compile and start this app:

```
cargo run
```
