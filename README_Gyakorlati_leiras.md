# Debezium CDC local gyakorlólab

Ez a projekt arra való, hogy egyszerűen lásd, hogyan lesz egy MySQL adatbázis-változásból Kafka esemény Debeziumon keresztül.

```text
MySQL
  ↓ binlog
Debezium MySQL Connector
  ↓
Kafka Connect
  ↓
Kafka
  ↓
Kafka UI
```

A MySQL adatokat Adminerben tudod nézni és módosítani.

---

## 1. Környezet indítása

A projekt gyökérkönyvtárában futtasd.

### Docker

```powershell
docker compose up -d
```

### Podman

```powershell
podman compose up -d
```

Ellenőrzés:

### Docker

```powershell
docker compose ps
```

### Podman

```powershell
podman ps
```

A fontos service-ek:

```text
mysql
connect
kafka
kafka-ui
adminer
```

---

## 2. Adminer megnyitása

Böngésző:

```text
http://localhost:18080
```

Belépés:

```text
System:    MySQL
Server:    mysql
Username:  bank
Password:  bank_lab_password
Database:  bank
```

A `Server` mező azért `mysql`, mert az Adminer és a MySQL külön konténerben futnak, és a Compose hálózaton service név alapján érik el egymást.

Belépés után két tábla van:

```text
ACCOUNT
PAYMENT
```

A `PAYMENT` táblában induláskor például:

```text
id=1
account_id=1001
amount=12500.00
status=COMPLETED
```

---

## 3. Kafka UI megnyitása

Böngésző:

```text
http://localhost:8081
```

Cluster:

```text
bank-lab
```

A Debezium connector regisztrálása előtt még nem feltétlen vannak `bank-cdc...` topicok.

---

## 4. Debezium connector konfiguráció

A connector konfigurációja itt van:

```text
connectors/bank-mysql.json
```

A legfontosabb részek:

```json
"connector.class": "io.debezium.connector.mysql.MySqlConnector"
```

Debezium MySQL connectort használunk.

```json
"database.hostname": "mysql"
```

Ezt a MySQL service-t figyeli.

```json
"database.include.list": "bank"
```

Csak a `bank` adatbázis érdekel.

```json
"table.include.list": "bank.ACCOUNT,bank.PAYMENT"
```

Csak ezt a két táblát figyeli.

```json
"topic.prefix": "bank-cdc"
```

Ezért lesznek ilyen topicok:

```text
bank-cdc.bank.ACCOUNT
bank-cdc.bank.PAYMENT
```

```json
"snapshot.mode": "initial"
```

Első induláskor a Debezium beolvassa a már meglévő sorokat is.

---

## 5. Connector regisztrálása

PowerShellből:

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://localhost:8083/connectors `
  -ContentType 'application/json' `
  -Body (Get-Content -Raw .\connectors\bank-mysql.json)
```

Ez elküldi a connector JSON-t a Kafka Connect REST API-nak.

Egyszerűen:

```text
bank-mysql.json
      ↓ HTTP POST
Kafka Connect
      ↓
bank-mysql Debezium connector
```

---

## 6. Connector státusz ellenőrzése

```powershell
Invoke-RestMethod http://localhost:8083/connectors/bank-mysql/status |
  ConvertTo-Json -Depth 10
```

Ezt keresd:

```text
connector.state = RUNNING
tasks[0].state   = RUNNING
```

Ha mindkettő `RUNNING`, a connector működik.

---

## 7. Snapshot megnézése

Kafka UI:

```text
Topics
  → bank-cdc.bank.PAYMENT
  → Messages
```

Az induláskor már létező `PAYMENT` sorok snapshot eventként jelennek meg.

Ilyet keress:

```json
{
  "before": null,
  "after": {
    "id": 1,
    "amount": "12500.00",
    "status": "COMPLETED"
  },
  "op": "r"
}
```

Jelentése:

```text
op = r        snapshot read
before = null nincs előző állapot
after         a sor aktuális állapota
```

Ez nem INSERT. A sor már létezett a Debezium indulása előtt.

A `source` blokkban látható többek között:

```text
db
table
file
pos
snapshot
```

MySQL esetén a `file + pos` mutatja, hol járunk a binlogban.

---

## 8. Valódi UPDATE kipróbálása

Adminerben futtasd:

```sql
UPDATE PAYMENT
SET
    status = 'SETTLED',
    amount = 13000.00,
    updated_at = NOW(3)
WHERE id = 1;
```

Utána menj vissza a Kafka UI-ba:

```text
bank-cdc.bank.PAYMENT
  → Messages
```

A legújabb eventben ezt kell látnod:

```text
op = u
```

és:

```json
"before": {
  "amount": "12500.00",
  "status": "COMPLETED"
},
"after": {
  "amount": "13000.00",
  "status": "SETTLED"
}
```

Ez már valódi CDC streaming event:

```text
SQL UPDATE
   ↓
MySQL commit
   ↓
binlog
   ↓
Debezium
   ↓
Kafka
```

A Debezium nem az Adminert figyeli. Ugyanez működne akkor is, ha a módosítást Java alkalmazás, batch vagy más SQL kliens végezné.

---

## 9. Mit jelent a `source` blokk?

Például:

```json
"source": {
  "db": "bank",
  "table": "PAYMENT",
  "snapshot": "false",
  "file": "mysql-bin.000003",
  "pos": 442,
  "server_id": 1,
  "gtid": "..."
}
```

Röviden:

```text
db/table     honnan jött az adat
snapshot     snapshot vagy streaming event
file + pos   MySQL binlog pozíció
server_id    MySQL server azonosító
gtid         tranzakció azonosító
```

Oracle CDC-nél ehhez hasonló szerepe van a redo/SCN/logpozíció gondolkodásnak.

---

## 10. Opcionális DELETE teszt

Adminer:

```sql
DELETE FROM PAYMENT
WHERE id = 1;
```

Kafka UI-ban két rekordot is láthatsz.

Először a Debezium DELETE eventet:

```text
op = d
before = törölt sor
after = null
```

Utána egy Kafka tombstone-t:

```text
key   = ugyanaz a kulcs
value = null
```

A tombstone külön Kafka record.

---

## 11. Hasznos címek

```text
Adminer:       http://localhost:18080
Kafka UI:      http://localhost:8081
Kafka Connect: http://localhost:8083
```

---

## 12. Környezet leállítása

### Docker

```powershell
docker compose down
```

### Podman

```powershell
podman compose down
```

Ha a volume-okat és minden local adatot is törölni akarod:

### Docker

```powershell
docker compose down -v
```

### Podman

```powershell
podman compose down -v
```

A `-v` után következő induláskor újra lefut a MySQL init, és a Debezium új snapshotot készíthet.

---

## Röviden mit láttunk?

```text
1. MySQL-ben létezett a PAYMENT sor
2. Debezium connector elindult
3. snapshotból op=r event készült
4. Adminerben UPDATE történt
5. MySQL binlogba bekerült a változás
6. Debezium kiolvasta
7. Kafka topicban op=u event jelent meg
8. before = régi állapot
9. after = új állapot
```

Ez a CDC alapfolyam.
