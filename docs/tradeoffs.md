# Trade-offs

Key design decisions and trade-offs:

1. Streaming vs batch: micro-batch reduces complexity but increases latency
2. Exactly-once vs at-least-once: at-least-once with idempotent writes
3. Glue vs ECS vs Lambda: Glue for managed Spark, ECS for control, Lambda for small tasks
4. Athena vs warehouse: Athena for cost efficiency and ad hoc analysis
