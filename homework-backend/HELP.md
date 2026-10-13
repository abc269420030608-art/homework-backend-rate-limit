# Setup and operational notes

## Prerequisites

- Java 21
- Docker Desktop / Docker Compose

## Start dependencies

From the project root, start MySQL, Redis, RocketMQ NameServer, broker, and
RocketMQ Console:

```powershell
docker compose up -d
```

The application connects to MySQL at `localhost:3306`, Redis at `localhost:6379`,
and RocketMQ at `localhost:9876`. Default database credentials are `taskuser` /
`taskpass`, with database name `taskdb`.

## Database scripts

`init.sql` initializes a fresh Docker MySQL volume. `src/main/resources/schema.sql`
is also run by Spring Boot (`spring.sql.init.mode=always`). Both define the rate
limit rules, operation logs, violation logs, and transactional-outbox table.

If an older local database already exists, recreate its Docker volume before
starting this version so that `rate_limits.rule_version` and `rate_limit_outbox`
are created:

```powershell
docker compose down -v
docker compose up -d
```

This removes local Docker database data.

## Run and verify

```powershell
.\mvnw.cmd spring-boot:run
```

Basic curl commands are in `README.md`. After an update, inspect pending Redis
synchronization work if needed:

```sql
SELECT id, api_key, rule_version, status, retry_count, last_error
FROM rate_limit_outbox
ORDER BY created_at DESC;
```
