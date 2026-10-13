#!/usr/bin/env python3
"""Validate a running bank CDC lab using only Python's standard library.

Start Compose and register bank-mysql before running this script. Only PAYMENT
rows 9901, 9902 and 9903 are reserved for this test. No volumes are reset.
Evidence is saved under artifacts/; a real failure produces a nonzero exit code.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts"
CONNECT = "http://localhost:8083"
CONNECTOR = "bank-mysql"
ACCOUNT_TOPIC = "bank-cdc.bank.ACCOUNT"
PAYMENT_TOPIC = "bank-cdc.bank.PAYMENT"
FIXTURE_IDS = (9901, 9902, 9903)
WAIT_SECONDS = 180


class LabTest:
    def __init__(self) -> None:
        ARTIFACTS.mkdir(exist_ok=True)
        self.result: dict[str, Any] = {
            "success": False,
            "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "checks": {},
            "evidence_files": [],
            "reserved_payment_ids": list(FIXTURE_IDS),
        }
        self.consumer_sequence = 0

    def artifact(self, name: str, data: Any, raw: bool = False) -> None:
        path = ARTIFACTS / name
        path.write_text(data if raw else json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        relative = str(path.relative_to(ROOT))
        if relative not in self.result["evidence_files"]:
            self.result["evidence_files"].append(relative)

    def checkpoint(self, name: str, data: Any = True) -> None:
        self.result["checks"][name] = data
        self.save_result()
        print(f"PASS {name}: {data}", flush=True)

    def save_result(self) -> None:
        (ARTIFACTS / "validation.json").write_text(
            json.dumps(self.result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )

    @staticmethod
    def command(args: list[str], *, sql: str | None = None, timeout: int = 60) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["docker", "compose", *args], cwd=ROOT, input=sql,
            text=True, capture_output=True, timeout=timeout, check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"docker compose {' '.join(args)} failed ({result.returncode}):\n"
                f"{result.stdout}\n{result.stderr}"
            )
        return result

    def sql(self, statement: str) -> str:
        return self.command(
            ["exec", "-T", "mysql", "sh", "-c",
             'MYSQL_PWD="$MYSQL_PASSWORD" exec mysql -ubank -Dbank --batch --skip-column-names'],
            sql=statement, timeout=45,
        ).stdout.strip()

    @staticmethod
    def http(url: str, *, as_json: bool = True) -> Any:
        with urllib.request.urlopen(url, timeout=10) as response:
            body = response.read().decode("utf-8")
            if response.status != 200:
                raise AssertionError(f"HTTP {response.status}: {url}")
            return json.loads(body) if as_json else body

    def wait_http(self, url: str, predicate: Callable[[Any], bool], *, as_json: bool = True) -> Any:
        # Compose considers services without healthchecks started before HTTP is ready.
        deadline = time.monotonic() + WAIT_SECONDS
        last: Any = None
        while time.monotonic() < deadline:
            try:
                last = self.http(url, as_json=as_json)
                if predicate(last):
                    return last
            except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError) as error:
                last = str(error)
            time.sleep(2)
        raise AssertionError(f"HTTP service did not become ready at {url}: {last}")

    def wait_running(self) -> dict[str, Any]:
        deadline = time.monotonic() + WAIT_SECONDS
        last: Any = None
        while time.monotonic() < deadline:
            try:
                last = self.http(f"{CONNECT}/connectors/{CONNECTOR}/status")
                tasks = last.get("tasks", [])
                if last.get("connector", {}).get("state") == "RUNNING" and tasks and all(
                    task.get("state") == "RUNNING" for task in tasks
                ):
                    return last
                failed = [entry for entry in [last.get("connector", {}), *tasks] if entry.get("state") == "FAILED"]
                if failed:
                    raise AssertionError(f"Connector/task failed: {json.dumps(failed)}")
            except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
                last = str(error)
            time.sleep(2)
        raise AssertionError(f"Connector and task did not become RUNNING: {last}")

    def topic_end(self, topic: str) -> int:
        output = self.command([
            "exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server", "kafka:9092", "--topic", topic, "--time", "-1",
        ]).stdout.strip()
        matches = re.findall(rf"^{re.escape(topic)}:(\d+):(\d+)$", output, re.MULTILINE)
        if len(matches) != 1 or matches[0][0] != "0":
            raise AssertionError(f"Expected exactly partition 0 for {topic}, got {output!r}")
        return int(matches[0][1])

    def consume(self, topic: str, offset: int | str, label: str) -> list[dict[str, Any]]:
        # Explicit partition + offset removes consumer-group state from the test.
        args = [
            "docker", "compose", "exec", "-T", "kafka",
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server", "kafka:9092", "--topic", topic,
            "--partition", "0", "--offset", str(offset), "--timeout-ms", "5000",
            "--property", "print.key=true", "--property", "print.offset=true",
        ]
        process = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, timeout=45, check=False)
        self.consumer_sequence += 1
        stem = f"{self.consumer_sequence:02d}-{label}"
        self.artifact(f"{stem}.stdout.txt", process.stdout, raw=True)
        self.artifact(f"{stem}.stderr.txt", process.stderr, raw=True)
        # Kafka exits 1 after the configured idle timeout. Do not hide other errors.
        if process.returncode != 0:
            exceptions = re.findall(r"(?:[A-Za-z_$][\w$]*\.)+[A-Za-z_$][\w$]*(?:Exception|Error)", process.stderr)
            expected_timeout = (
                process.returncode == 1
                and "org.apache.kafka.common.errors.TimeoutException" in process.stderr
                and all(exception == "org.apache.kafka.common.errors.TimeoutException" for exception in exceptions)
                and "Processed a total of " in process.stderr
            )
            if not expected_timeout:
                raise AssertionError(f"Kafka consumer failed: {process.stderr}\n{process.stdout}")
        for line in process.stderr.splitlines():
            if re.search(r"\b(?:ERROR|WARN)\b", line) and "Error processing message, terminating consumer process" not in line:
                raise AssertionError(f"Unexpected Kafka consumer diagnostic: {line}")
        records = []
        for line in process.stdout.splitlines():
            match = re.fullmatch(r"Offset:(\d+)\t([^\t]+)\t(.*)", line)
            if not match:
                raise AssertionError(f"Unexpected Kafka output format: {line!r}")
            records.append({"offset": int(match[1]), "key": json.loads(match[2]), "value": json.loads(match[3])})
        return records

    def wait_records(
        self, topic: str, offset: int | str, label: str,
        predicate: Callable[[list[dict[str, Any]]], bool],
    ) -> list[dict[str, Any]]:
        deadline = time.monotonic() + WAIT_SECONDS
        found: dict[int, dict[str, Any]] = {}
        next_offset = offset
        while time.monotonic() < deadline:
            batch = self.consume(topic, next_offset, label)
            for record in batch:
                found[record["offset"]] = record
            if batch:
                next_offset = max(record["offset"] for record in batch) + 1
            records = [found[position] for position in sorted(found)]
            if predicate(records):
                self.artifact(f"{label}.records.json", records)
                return records
        raise AssertionError(f"Expected events did not arrive for {label}; collected {len(found)} records")

    @staticmethod
    def by_id(records: list[dict[str, Any]], row_id: int) -> list[dict[str, Any]]:
        return [record for record in records if record["key"] == {"id": row_id}]

    @staticmethod
    def ops(records: list[dict[str, Any]], row_id: int) -> list[str | None]:
        return [record["value"]["op"] if record["value"] is not None else None
                for record in LabTest.by_id(records, row_id)]

    @staticmethod
    def validate_envelope(record: dict[str, Any], table: str, snapshot: bool) -> None:
        event = record["value"]
        assert event is not None, "Envelope validator received a tombstone"
        assert {"before", "after", "source", "op", "ts_ms"} <= event.keys(), event
        assert isinstance(event["ts_ms"], int) and event["ts_ms"] > 0, event
        source = event["source"]
        assert source.get("connector") == "mysql" and source.get("db") == "bank" and source.get("table") == table, source
        assert isinstance(source.get("file"), str) and isinstance(source.get("pos"), int), source
        assert "ts_ms" in source, source
        if snapshot:
            # Debezium 3 adds boundaries for the snapshot/data collection.
            assert source.get("snapshot") in (
                True, "true", "first", "first_in_data_collection",
                "last_in_data_collection", "last",
            ), source
        else:
            assert source.get("snapshot") in (False, "false"), source
        if event["op"] in ("r", "c"):
            assert event["before"] is None and event["after"] is not None, event
            row = event["after"]
        elif event["op"] == "u":
            assert event["before"] is not None and event["after"] is not None, event
            row = event["after"]
        elif event["op"] == "d":
            assert event["before"] is not None and event["after"] is None, event
            row = event["before"]
        else:
            raise AssertionError(f"Unexpected operation: {event['op']}")
        assert record["key"] == {"id": row["id"]}, record

    def offset_state(self) -> dict[str, Any]:
        state = self.http(f"{CONNECT}/connectors/{CONNECTOR}/offsets")
        offsets = state.get("offsets", [])
        assert offsets, f"Connector offset endpoint returned no committed offsets: {state}"
        assert any(entry.get("partition", {}).get("server") == "bank-cdc" for entry in offsets), state
        assert all(entry.get("offset", {}).get("file") and isinstance(entry["offset"].get("pos"), int) for entry in offsets), state
        return state

    def stable_offsets(self) -> dict[str, Any]:
        deadline = time.monotonic() + 30
        previous = None
        # Two seconds spans two configured flush intervals; check twice for stability.
        time.sleep(2)
        while time.monotonic() < deadline:
            current = self.offset_state()
            if current == previous:
                return current
            previous = current
            time.sleep(2)
        raise AssertionError("Committed connector offsets never became stable")

    def clean_reserved_rows(self) -> None:
        existing = [int(line) for line in self.sql("SELECT id FROM PAYMENT WHERE id IN (9901,9902,9903);").splitlines() if line]
        if not existing:
            return
        boundary = self.topic_end(PAYMENT_TOPIC)
        self.sql("DELETE FROM PAYMENT WHERE id IN (9901,9902,9903);")
        self.wait_records(
            PAYMENT_TOPIC, boundary, "cleanup",
            lambda records: all("d" in self.ops(records, row_id) and None in self.ops(records, row_id) for row_id in existing),
        )

    def run(self) -> None:
        status = self.wait_running()
        self.artifact("status-initial.json", status)
        self.checkpoint("connector_and_task_running")

        self.wait_http("http://localhost:8080", lambda page: "Adminer" in page and "mysql" in page.lower(), as_json=False)
        clusters = self.wait_http("http://localhost:8081/api/clusters", lambda data: isinstance(data, list) and any(
            cluster.get("name") == "bank-lab" and cluster.get("status") == "online" for cluster in data
        ))
        self.artifact("kafka-ui-clusters.json", clusters)
        self.checkpoint("browser_services", {"adminer": True, "kafka_ui_cluster_online": True})

        for table, topic, expected_ids in (("ACCOUNT", ACCOUNT_TOPIC, (1001, 1002)), ("PAYMENT", PAYMENT_TOPIC, (1, 2))):
            records = self.wait_records(
                topic, "earliest", f"snapshot-{table.lower()}",
                lambda rows, ids=expected_ids: all("r" in self.ops(rows, row_id) for row_id in ids),
            )
            snapshots = [record for record in records if record["value"] is not None and record["value"].get("op") == "r"]
            for record in snapshots:
                self.validate_envelope(record, table, True)
            self.checkpoint(f"snapshot_{table}", {"op": "r", "expected_ids": list(expected_ids), "snapshot_count": len(snapshots)})

        self.clean_reserved_rows()
        boundary = self.topic_end(PAYMENT_TOPIC)
        self.sql("""
INSERT INTO PAYMENT (id,account_id,amount,currency,status) VALUES (9901,1001,88.00,'HUF','PENDING');
UPDATE PAYMENT SET amount=123.45,status='COMPLETED' WHERE id=9901;
DELETE FROM PAYMENT WHERE id=9901;
""")
        live = self.wait_records(PAYMENT_TOPIC, boundary, "live-payment", lambda rows: self.ops(rows, 9901) == ["c", "u", "d", None])
        changed = self.by_id(live, 9901)
        assert len(changed) == 4 and self.ops(live, 9901) == ["c", "u", "d", None], changed
        for record in changed[:-1]:
            self.validate_envelope(record, "PAYMENT", False)
        inserted, updated, deleted = [record["value"] for record in changed[:3]]
        assert inserted["after"]["status"] == "PENDING" and Decimal(inserted["after"]["amount"]) == Decimal("88.00"), inserted
        assert updated["before"]["status"] == "PENDING" and updated["after"]["status"] == "COMPLETED", updated
        assert Decimal(updated["before"]["amount"]) == Decimal("88.00") and Decimal(updated["after"]["amount"]) == Decimal("123.45"), updated
        assert updated["before"].keys() == updated["after"].keys() == inserted["after"].keys(), updated
        assert deleted["before"] == updated["after"] and deleted["after"] is None, deleted
        assert changed[3]["value"] is None and changed[3]["key"] == changed[2]["key"], changed
        self.checkpoint("insert_update_delete_tombstone", {"id": 9901, "operations": ["c", "u", "d", None], "start_offset": boundary})

        seed_boundary = self.topic_end(PAYMENT_TOPIC)
        self.sql("INSERT INTO PAYMENT (id,account_id,amount,currency,status) VALUES (9902,1001,10.00,'HUF','PENDING');")
        seed = self.wait_records(PAYMENT_TOPIC, seed_boundary, "recovery-seed", lambda rows: self.ops(rows, 9902) == ["c"])
        self.validate_envelope(self.by_id(seed, 9902)[0], "PAYMENT", False)
        before_offsets = self.stable_offsets()
        self.artifact("connector-offsets-before-restart.json", before_offsets)
        recovery_boundary = self.topic_end(PAYMENT_TOPIC)
        self.artifact("recovery-boundary.json", {"topic": PAYMENT_TOPIC, "partition": 0, "next_offset": recovery_boundary})

        # Always restart Connect, including after a failed SQL/assertion operation.
        stopped = False
        try:
            # Even an interrupted/failed stop can have stopped the container.
            stopped = True
            self.command(["stop", "-t", "45", "connect"], timeout=90)
            services = set(self.command(["ps", "--status", "running", "--services"]).stdout.splitlines())
            assert "connect" not in services and {"mysql", "kafka", "adminer", "kafka-ui"} <= services, services
            self.sql("""
UPDATE PAYMENT SET amount=20.00,status='COMPLETED' WHERE id=9902;
INSERT INTO PAYMENT (id,account_id,amount,currency,status) VALUES (9903,1002,30.00,'HUF','PENDING');
DELETE FROM PAYMENT WHERE id=9903;
""")
            assert self.topic_end(PAYMENT_TOPIC) == recovery_boundary, "PAYMENT events appeared while Connect was stopped"
            self.checkpoint("mysql_changes_while_only_connect_stopped", {"services_running": sorted(services), "kafka_boundary_unchanged": True})
        finally:
            if stopped:
                self.command(["start", "connect"], timeout=60)

        recovered_status = self.wait_running()
        self.artifact("status-after-restart.json", recovered_status)
        recovery = self.wait_records(
            PAYMENT_TOPIC, recovery_boundary, "recovery-payment",
            lambda rows: self.ops(rows, 9902) == ["u"] and self.ops(rows, 9903) == ["c", "d", None],
        )
        assert all(record["value"] is None or record["value"]["op"] != "r" for record in recovery), recovery
        for record in recovery:
            if record["value"] is not None:
                self.validate_envelope(record, "PAYMENT", False)
        update = self.by_id(recovery, 9902)[0]["value"]
        assert update["before"]["status"] == "PENDING" and update["after"]["status"] == "COMPLETED", update
        assert Decimal(update["before"]["amount"]) == Decimal("10.00") and Decimal(update["after"]["amount"]) == Decimal("20.00"), update
        inserted_deleted = self.by_id(recovery, 9903)
        assert inserted_deleted[0]["value"]["after"] == inserted_deleted[1]["value"]["before"], inserted_deleted
        assert inserted_deleted[2]["value"] is None and inserted_deleted[2]["key"] == inserted_deleted[1]["key"], inserted_deleted
        after_offsets = self.stable_offsets()
        self.artifact("connector-offsets-after-restart.json", after_offsets)
        assert after_offsets != before_offsets, "Committed connector offset did not advance after recovery"
        self.checkpoint("restart_offset_recovery", {
            "updated_id": 9902, "inserted_deleted_id": 9903,
            "snapshot_events_after_boundary": 0, "offset_advanced": True,
            "recovered_operations": {"9902": self.ops(recovery, 9902), "9903": self.ops(recovery, 9903)},
            "start_offset": recovery_boundary,
        })

        # Leave only the original bank example records, preserving other user rows.
        self.clean_reserved_rows()
        self.checkpoint("test_rows_cleaned_up")
        self.result["success"] = True
        self.result["completed_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.save_result()
        print("All real Docker CDC integration checks passed. Evidence: artifacts/validation.json", flush=True)


def main() -> int:
    test = LabTest()
    try:
        test.run()
        return 0
    except (AssertionError, RuntimeError, subprocess.SubprocessError, urllib.error.URLError, ValueError, OSError) as error:
        test.result["error"] = str(error)
        test.save_result()
        print(f"FAIL: {error}\nEvidence: artifacts/validation.json", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
