# FlowEngine MVP — local dev stack

## What this proves (T5 + T8)

Running this brings up:
- **RabbitMQ** (with management UI) — the event backbone for Phase 1
- **Postgres** — placeholder DB, real per-service schemas come in Phase 3-5
- **dummy-service** — just confirms the whole stack boots together with one command

## Run it

```bash
docker compose up
```

First run pulls images, so it'll take a minute. Subsequent runs are fast.

## Verify T8's acceptance check

1. Run `docker compose up -d`
2. Open http://localhost:15672 in a browser
3. Log in with `flowengine` / `flowengine_dev`
4. You should see the RabbitMQ management dashboard load — Overview tab, no errors

If the page loads, T8 is done. Check it off in the tracker.

## Stop it

```bash
docker compose down
```

Add `-v` if you want to wipe the data volumes too (`docker compose down -v`).

## Next up

- **T9**: tiny publish/subscribe test script against this RabbitMQ instance (now correctly depends on T8, not itself)
- **T6/T7**: event taxonomy + JSON schemas — needed before T9's test script has a real payload to publish
