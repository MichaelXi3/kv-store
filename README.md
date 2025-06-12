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
Current Implementation:
- K/V are string only
- memtable is implemented as MemTable

Future Enhancement:
- K/V can be any type
- memtable implemented using skipList
- Multi-thread flushing
