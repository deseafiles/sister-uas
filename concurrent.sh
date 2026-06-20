#!/bin/bash

for i in {1..10}
do
(
curl -X POST http://localhost:8080/publish \
-H "Content-Type: application/json" \
-d '{
  "events":[{
    "topic":"demo.concurrent",
    "event_id":"same-id",
    "source":"worker",
    "timestamp":"2024-01-15T10:00:00Z",
    "payload":{"worker":"test"}
  }]
}'
) &
done

wait
