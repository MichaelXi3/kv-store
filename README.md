### KV Storage Workflow
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

### KV Storage Write Steps
```
Timeline: Write   →    Active   →    Immutable   →   SSTable
        [WAL Write] [WAL Protects] [WAL Protects] [WAL Can Be Deleted]

Phase 1: store.put("key", "value")
         ├─ Active MemTable ✓
         └─ WAL Entry ✓

Phase 2: MemTable full, becomes immutable  
         ├─ Immutable MemTable ✓  
         ├─ New Active MemTable (empty)
         └─ WAL Entry ✓ (still needed)

Phase 3: Flushing to SSTable
         ├─ Immutable MemTable ✓
         ├─ SSTable (writing...) 
         └─ WAL Entry ✓ (still needed)

Phase 4: SSTable complete
         ├─ SSTable ✓ (persisted)
         └─ WAL Entry (can be deleted)
```

### Implementation Details
Current Implementation Limitations:
- K/V are string only
- memtable is implemented as MemTable

ToDos util MVP:
- KVsore ReaSSTableReader ✓
- Build SSTable min/max_key index during KVStore constructor ✓
- Delete: tombstone ✓
- Compaction: SSTable compaction when table cnt exceeds threshold

Future Enhancement:
- K/V can be any type
- memtable implemented using skipList
- Multi-thread flushing
- Logging level configs
