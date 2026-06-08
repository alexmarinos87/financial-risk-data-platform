db = db.getSiblingDB("risk_source");

db.background_checks.drop();
db.background_checks.insertMany([
  {
    _id: ObjectId("665f4a731d7f2d9f0a9d0001"),
    candidateId: "cand_123",
    check: {
      id: "check_987",
      type: "employment_reference",
      status: "completed",
    },
    employer: {
      name: "Example Ltd",
      country: "GB",
    },
    timestamps: {
      createdAt: ISODate("2026-06-04T09:01:00Z"),
      updatedAt: ISODate("2026-06-04T09:05:30Z"),
      completedAt: ISODate("2026-06-04T09:05:00Z"),
    },
    source: "application",
  },
  {
    _id: ObjectId("665f4a731d7f2d9f0a9d0002"),
    candidateId: "cand_456",
    check: {
      id: "check_988",
      type: "right_to_work",
      status: "in_progress",
    },
    employer: {
      name: "Sample Co",
      country: "GB",
    },
    timestamps: {
      createdAt: ISODate("2026-06-04T10:15:00Z"),
      updatedAt: ISODate("2026-06-04T10:18:20Z"),
      completedAt: null,
    },
    source: "application",
  },
]);

db.background_checks.createIndex({ "timestamps.updatedAt": 1 });
db.background_checks.createIndex({ "check.id": 1 }, { unique: true });

db.market_events_source.drop();
db.market_events_source.insertMany([
  {
    _id: ObjectId("665f4a731d7f2d9f0a9d0101"),
    eventId: "evt-1",
    instrument: {
      symbol: "AAPL",
    },
    trade: {
      price: 100.0,
      volume: 10,
    },
    timestamps: {
      event: ISODate("2025-01-20T10:01:00Z"),
      ingest: ISODate("2025-01-20T10:01:05Z"),
    },
    provider: "stooq",
  },
  {
    _id: ObjectId("665f4a731d7f2d9f0a9d0102"),
    eventId: "evt-2",
    instrument: {
      symbol: "AAPL",
    },
    trade: {
      price: 101.0,
      volume: 11,
    },
    timestamps: {
      event: ISODate("2025-01-20T10:02:00Z"),
      ingest: ISODate("2025-01-20T10:02:04Z"),
    },
    provider: "stooq",
  },
  {
    _id: ObjectId("665f4a731d7f2d9f0a9d0103"),
    eventId: "evt-2",
    instrument: {
      symbol: "AAPL",
    },
    trade: {
      price: 101.0,
      volume: 11,
    },
    timestamps: {
      event: ISODate("2025-01-20T10:02:00Z"),
      ingest: ISODate("2025-01-20T10:02:04Z"),
    },
    provider: "stooq",
  },
]);

db.market_events_source.createIndex({ eventId: 1 });
db.market_events_source.createIndex({ "timestamps.ingest": 1 });
