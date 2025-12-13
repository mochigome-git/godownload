## /download

curl -X POST http://localhost:8081/download \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer eyJhbGc..." \
  -d '{
    "tables": [
      {
       "schema": "analytics",
        "table": "realtime_metrics",
        "in_column": "lot_id",
        "select": "*",
        "in_values": ["55c46b9f-2ec9-4254-af1d-7f2380227107", "b80e84a1-13d2-4b7d-a6f4-87cc2c3a4868", "9e04a531-a493-4506-aa1c-ee78fe557714",
        "5fae8624-1916-4425-bd5a-f62029a7cddb"],
        "order_by": [{ "column": "created_at", "ascending": true }]
      }
    ],
    "group_by": "machine_id",
    "file_name": "metrics_by_machine"
    "batch_size": 20,
    "concurrency": 5,
    "is_five_min": false
  }'



## /export

curl -X POST http://localhost:8081/export \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer eyJhbGciO..." \
  -d '{
    "tables": [
      {
       "schema": "analytics",
        "table": "realtime_metrics",
        "in_column": "lot_id",
        "select": "*",
        "in_values": ["55c46b9f-2ec9-4254-af1d-7f2380227107", "b80e84a1-13d2-4b7d-a6f4-87cc2c3a4868", "9e04a531-a493-4506-aa1c-ee78fe557714",
        "5fae8624-1916-4425-bd5a-f62029a7cddb"],
        "order_by": [{ "column": "created_at", "ascending": true }]
      },
            {
       "schema": "analytics",
        "table": "realtime_metrics",
        "in_column": "lot_id",
        "select": "*",
        "in_values": ["e533d7ff-eb85-40ba-8221-b39cf2ebec65"],
        "order_by": [{ "column": "created_at", "ascending": true }]
      }
    ],
    "group_by": "machine_id",
    "file_name": "metrics_by_machine",
    "is_five_min": false
  }' \
   --output export.xlsx
