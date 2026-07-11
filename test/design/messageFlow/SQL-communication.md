┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              SQLITE COMMUNICATION FLOW                                         │
├─────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                 │
│  Child Process                    Parent (Orchestrator)          SQLite Server                  │
│  ─────────────                    ────────────────────          ─────────────                  │
│                                                                                                 │
│  SQLiteCommWorker                                                                               │
│       │                                                                                         │
│       │ 1. SQLITE_REQUEST ────────────────────────────────────────────────────────────────────►│
│       │    { data: { op: 'append', queue: 'browser_queue', jobId: 'NSE-OPTION', payload } }   │
│       │                                                                                         │
│       │                             2. Forwards to SQLite Server ────────────────────────────►│
│       │                                 { op: 'append', queue, jobId, payload }               │
│       │                                                                                         │
│       │                                                        3. Routes to Write Queue       │
│       │                                                                                         │
│       │                                                        4. Write Worker processes      │
│       │                                                           calls sql-queries.append()  │
│       │                                                                                         │
│       │                                                        5. SQLite executes INSERT       │
│       │                                                                                         │
│       │                             6. SQLITE_RESPONSE ◄──────────────────────────────────────│
│       │                                { jobId: 'NSE-OPTION', success: true }                │
│       │                                                                                         │
│       │ 7. Matches response by jobId                                                           │
│       │    Resolves promise                                                                     │
│       │                                                                                         │
│  ✅ Operation complete                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘