# WP6.5 合并后审计链路运行记录

以下是 2026-11-23 在 Docker Desktop 上以合成数据运行的完整结果。

```json
{
  "revision": "ace69f182470403bd7d2ef57a74251afe143eaae",
  "dirty": false,
  "writer_image": "sha256:2d8e30b6c6167b675b27743f7b6caa50c6dab9d008a58139698f3b1918c6ee34",
  "races": {
    "identical": {
      "writer_http": [
        204,
        204
      ],
      "catalog_commits": [
        {
          "parents": [
            null
          ],
          "http": 200
        },
        {
          "parents": [
            null
          ],
          "http": 409
        }
      ],
      "table": {
        "snapshot_id": 3151821634096979227,
        "snapshot_count": 1,
        "properties": {
          "write.metadata.delete-after-commit.enabled": "false",
          "write.format.default": "parquet",
          "history.expire.max-snapshot-age-ms": "9223372036854775807",
          "history.expire.min-snapshots-to-keep": "2147483647",
          "write.parquet.compression-codec": "zstd",
          "write.metadata.previous-versions-max": "999999"
        },
        "metadata_location": "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/metadata/00001-01a208cc-c283-7ca0-8b81-b82842c4931a.gz.metadata.json",
        "data_files": [
          "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-400a4e94-9704-46bd-827b-e39872c7d566.parquet"
        ],
        "row_count": 1
      }
    },
    "overlap": {
      "writer_http": [
        204,
        204
      ],
      "catalog_commits": [
        {
          "parents": [
            3151821634096979227
          ],
          "http": 409
        },
        {
          "parents": [
            3151821634096979227
          ],
          "http": 200
        }
      ],
      "table": {
        "snapshot_id": 6620944166827447072,
        "snapshot_count": 3,
        "properties": {
          "write.format.default": "parquet",
          "write.metadata.previous-versions-max": "999999",
          "history.expire.min-snapshots-to-keep": "2147483647",
          "history.expire.max-snapshot-age-ms": "9223372036854775807",
          "write.metadata.delete-after-commit.enabled": "false",
          "write.parquet.compression-codec": "zstd"
        },
        "metadata_location": "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/metadata/00003-01a208cc-c5cb-7ab1-b821-e8f2ddde669b.gz.metadata.json",
        "data_files": [
          "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-0fcaf189-387f-4b1a-9991-aaf70d579028.parquet",
          "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-37a795c1-dded-46a2-9758-31e52f3ca669.parquet",
          "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-400a4e94-9704-46bd-827b-e39872c7d566.parquet"
        ],
        "row_count": 4
      }
    },
    "conflict": {
      "writer_http": [
        204,
        409
      ],
      "catalog_commits": [
        {
          "parents": [
            6620944166827447072
          ],
          "http": 200
        },
        {
          "parents": [
            6620944166827447072
          ],
          "http": 409
        }
      ],
      "table": {
        "snapshot_id": 3933361429459009296,
        "snapshot_count": 4,
        "properties": {
          "write.metadata.previous-versions-max": "999999",
          "write.parquet.compression-codec": "zstd",
          "history.expire.max-snapshot-age-ms": "9223372036854775807",
          "history.expire.min-snapshots-to-keep": "2147483647",
          "write.format.default": "parquet",
          "write.metadata.delete-after-commit.enabled": "false"
        },
        "metadata_location": "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/metadata/00004-01a208cc-c766-7061-adcc-4b9c1e0d78c5.gz.metadata.json",
        "data_files": [
          "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-0fcaf189-387f-4b1a-9991-aaf70d579028.parquet",
          "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-37a795c1-dded-46a2-9758-31e52f3ca669.parquet",
          "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-400a4e94-9704-46bd-827b-e39872c7d566.parquet",
          "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-e5a17786-1c65-463b-954b-89ffadd90bab.parquet"
        ],
        "row_count": 6
      }
    }
  },
  "lost_ack": {
    "events": [
      {
        "record_id": "55ce29f3-025e-4358-93a1-03f3bb32a384",
        "event_id": "65366883-b956-50cd-bb32-9177c706e604",
        "status": "approved",
        "partition": 0,
        "offset": 0,
        "same_transaction_receipt_outbox_insert_delete": true
      },
      {
        "record_id": "d1d5a817-0b12-4775-a434-cae994779666",
        "event_id": "ed7df664-1c47-5bcd-92dd-d31010e7a413",
        "status": "denied",
        "partition": 0,
        "offset": 1,
        "same_transaction_receipt_outbox_insert_delete": true
      }
    ],
    "sink_exit": 1,
    "group": "audit-sink",
    "offsets": [
      1,
      2
    ],
    "kafka_end": 2,
    "writer_requests": [
      [
        "46ac832b103055331ee5d2f1c2b25ac8d796e37b8657669600cbdf49c0651d09",
        204
      ],
      [
        "9921fb81a3ce9675c625ee7f684f37bd5b3b3e283e23f713c7665feed702888f",
        204
      ],
      [
        "9921fb81a3ce9675c625ee7f684f37bd5b3b3e283e23f713c7665feed702888f",
        204
      ]
    ],
    "table_after_commit_and_after_replay": {
      "snapshot_id": 2681042823184156642,
      "snapshot_count": 6,
      "properties": {
        "history.expire.max-snapshot-age-ms": "9223372036854775807",
        "write.metadata.previous-versions-max": "999999",
        "write.parquet.compression-codec": "zstd",
        "history.expire.min-snapshots-to-keep": "2147483647",
        "write.metadata.delete-after-commit.enabled": "false",
        "write.format.default": "parquet"
      },
      "metadata_location": "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/metadata/00006-01a208cc-fa8c-78d3-9941-2f13c7027874.gz.metadata.json",
      "data_files": [
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-0fcaf189-387f-4b1a-9991-aaf70d579028.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-37a795c1-dded-46a2-9758-31e52f3ca669.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-400a4e94-9704-46bd-827b-e39872c7d566.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-e5a17786-1c65-463b-954b-89ffadd90bab.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2026-11-23/00000-0-84775728-7da3-4c1e-903e-b1a0877cc70f.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2026-11-23/00000-0-aef771c7-5282-4a3d-af06-8cb6485d9d92.parquet"
      ],
      "row_count": 8
    }
  },
  "retention_guard": {
    "finite_retention_http": 409,
    "snapshot_unchanged": true
  },
  "catalog_failures": {
    "outage_http": 503,
    "response_lost_http": 503,
    "replay_http": 204,
    "table": {
      "snapshot_id": 9097467298563130268,
      "snapshot_count": 7,
      "properties": {
        "write.metadata.delete-after-commit.enabled": "false",
        "write.format.default": "parquet",
        "history.expire.max-snapshot-age-ms": "9223372036854775807",
        "write.parquet.compression-codec": "zstd",
        "history.expire.min-snapshots-to-keep": "2147483647",
        "write.metadata.previous-versions-max": "999999"
      },
      "metadata_location": "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/metadata/00007-01a208cd-3808-7311-acf5-1b674e9a2287.gz.metadata.json",
      "data_files": [
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-0fcaf189-387f-4b1a-9991-aaf70d579028.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-37a795c1-dded-46a2-9758-31e52f3ca669.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-400a4e94-9704-46bd-827b-e39872c7d566.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-837ff059-0ea9-4e17-9090-8cbc526193f8.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-e5a17786-1c65-463b-954b-89ffadd90bab.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2026-11-23/00000-0-84775728-7da3-4c1e-903e-b1a0877cc70f.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2026-11-23/00000-0-aef771c7-5282-4a3d-af06-8cb6485d9d92.parquet"
      ],
      "row_count": 9
    }
  },
  "storage_failure": {
    "s3_put_http": 403,
    "catalog_http": 200,
    "writer_http": 503,
    "replay_http": 204,
    "previous_snapshot": 9097467298563130268,
    "table": {
      "snapshot_id": 8471282407320935228,
      "snapshot_count": 8,
      "properties": {
        "write.format.default": "parquet",
        "write.metadata.previous-versions-max": "999999",
        "write.metadata.delete-after-commit.enabled": "false",
        "write.parquet.compression-codec": "zstd",
        "history.expire.min-snapshots-to-keep": "2147483647",
        "history.expire.max-snapshot-age-ms": "9223372036854775807"
      },
      "metadata_location": "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/metadata/00008-01a208cd-3da0-7c00-9f6b-7457e810d844.gz.metadata.json",
      "data_files": [
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-0fcaf189-387f-4b1a-9991-aaf70d579028.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-1b67b776-5819-4ab1-86b5-6c5a20374508.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-37a795c1-dded-46a2-9758-31e52f3ca669.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-400a4e94-9704-46bd-827b-e39872c7d566.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-837ff059-0ea9-4e17-9090-8cbc526193f8.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2023-11-14/00000-0-e5a17786-1c65-463b-954b-89ffadd90bab.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2026-11-23/00000-0-84775728-7da3-4c1e-903e-b1a0877cc70f.parquet",
        "s3://audit-fixture/01a208cc-9331-7a30-a10d-381b3396c2d6/01a208cc-9337-7521-926a-8c0540f631c4/data/at_day=2026-11-23/00000-0-aef771c7-5282-4a3d-af06-8cb6485d9d92.parquet"
      ],
      "row_count": 10
    }
  },
  "container_ids": [
    "6df934d6da733c385dc4bb4b14b7eff8f20a04ca54aee473ddec43c2ff80dc88",
    "d8b109ce887e170ddc02783d08d143f8ecdefeaed75635b118a6aad2c5feaf18"
  ],
  "result": "passed",
  "cleaned_up": true,
  "helm_revision": "ace69f182470403bd7d2ef57a74251afe143eaae",
  "debezium": "2.7.0.Final"
}
```
