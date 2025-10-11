add soft delete
add ttl
add exists
For very high-throughput, batched writes + WAL mode could improve speed:
PRAGMA journal_mode=WAL;
