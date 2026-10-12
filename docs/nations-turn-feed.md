# Nations pending-turn feed

External notification services can use one read-only WebSocket instead of a
connection and repeated full game-state requests for every watched match.
The server knows nothing about subscriptions, Discord accounts, channels, or
delivery preferences. Filtering and notification delivery belong to the client.

## Deployment

Set `NATIONS_TURN_FEED_ENABLED=TRUE` in the ASGI process environment and restart
the application. The endpoint is disabled by default. Use the existing shared
Redis channel layer when running multiple ASGI processes; an in-memory layer
only works within one process. No database migration is needed.

Connect to `wss://games.tabony.net/ws/nations/turns/`. This is an anonymous public
feed of game IDs and usernames already visible in public game listings, not a
player-authenticated or Discord-specific endpoint. The operator should enable
it only if publishing this aggregate public feed is acceptable.

## Protocol

The server immediately sends a complete snapshot of all started, unfinished
games with a pending player, including ongoing archived games:

```json
{"type":"snapshot","turns":[{"game":"3038","player":"Kayser"}]}
```

Replace the client's pending-turn map with this snapshot. IDs are strings and
usernames preserve their exact case. Match titles and participant lists are not
included; clients can keep their existing public metadata cache if needed.

After a committed turn-owner change or game completion/deletion:

```json
{"type":"turn","game":"3038","player":"Suddoman"}
{"type":"turn","game":"3038","player":null}
```

Set the game's pending player, or remove it on `null`. Repeated messages are
possible and must be treated as idempotent state updates, not unconditional
notification commands. Moves that keep the same player do not broadcast.
Starting a game also generates an update.

Send JSON `null` periodically (for example every 30 seconds):

```json
{"type":"pong"}
```

The reply proves liveness and renews channel-layer group membership without a
database scan or engine replay. Other incoming JSON closes the connection with
code 1008; the endpoint accepts no moves, chat, settings, or subscriptions.

Reconnect with bounded backoff after disconnects or missed heartbeat replies.
Each connection receives a fresh snapshot. There is no durable event log:
updates during downtime are recovered as current pending turns, not historical
turns. Channels delivery is best-effort; a periodic reconnect can reconcile a
rare dropped broadcast. The feed exposes current state rather than every
intermediate turn if a game advances faster than the consumer processes events.

## Cost and integration

Each connection does one joined database query for its initial snapshot. Each
saved Match adds one small query for the previous player/game-over fields when
the feed is enabled. Changes send a small marker through the existing channel
layer; each feed connection reads the current stored player once per marker.
Reading the latest owner also prevents an event queued before the initial
snapshot from restoring an obsolete player. No rules-engine reconstruction,
chat log, notes, or full game state is sent. Disabled mode adds no save query.

Signals cover normal `Match.save()` and deletion, including admin saves.
Bulk `QuerySet.update()` and raw SQL bypass save signals; code using them to
change turn ownership must explicitly call `publish_turn_change(game_id)`
after the update, within the same transaction. Failed publication is logged
and never raises into a successfully saved game move.

This PR provides the server feed only. The external notifier must separately
switch from per-match sockets to this protocol and retain its own deduplication,
subscription storage, metadata discovery, Discord delivery and retry logic.

## Tests

With the repository's `requirements.txt` installed:

```sh
python -m django test Nations --settings=Nations.test_settings
```

The isolated settings use SQLite and the in-memory channel layer, without
production secrets, a running server, Redis, or the rules engine.
