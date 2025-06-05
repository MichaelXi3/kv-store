```
Client ──▶ KV Store API (put/get)
           │
           ▼
        MemTable (in-memory)
           │
           ├───▶ WAL (log_writer)
           └───▶ Flush thread
                    │
                    ▼
            SSTable (sorted segment files)
                    ▼
            Compaction thread
```
