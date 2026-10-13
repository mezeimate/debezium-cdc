# Debezium CDC banki gyakorlólab

Ez a projekt arra való, hogy helyben, egyszerűen kipróbáld a CDC működését:

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

A MySQL adatokat **Adminerben** tudod megnézni és módosítani, a keletkező CDC eseményeket pedig **Kafka UI-ban**.

A lab helyi fejlesztői környezet. Ismert gyakorlójelszavakat és titkosítás nélküli kapcsolatokat használ, ezért production célra nem való.

---

## 1. Mire van szükség?

A lab futtatható **Dockerrel vagy Podmannel**.

### Docker

Ellenőrzés:

```powershell
docker --version
docker compose version
```

### Podman

Ellenőrzés:

```powershell
podman --version
podman compose version
```

A használt host portok:

```text
3306   MySQL
18080  Adminer
8081   Kafka UI
8083   Kafka Connect REST API
9092   Kafka
```

---

## 2. A környezet elindítása

A parancsokat a projekt gyökérkönyvtárában futtasd, ahol a `docker-compose.yml` található.

### Docker

```powershell
docker compose up -d
docker compose ps
```

### Podman

```powershell
podman compose up -d
podman ps
```

A fontos konténerek:

```text
mysql
kafka
connect
kafka-ui
adminer
```

A MySQL és a Kafka Connect állapota legyen `Healthy`.

Az első indítás tovább tarthat, mert le kell tölteni az image-eket.

---

## 3. Mit indít el a Compose?

| Szolgáltatás | Feladat | Helyi cím |
| --- | --- | --- |
| MySQL | Source adatbázis | `localhost:3306` |
| Adminer | MySQL webes kezelőfelület | <http://localhost:18080> |
| Kafka | CDC események tárolása | `localhost:9092` |
| Kafka Connect + Debezium | MySQL változások figyelése és Kafka felé továbbítása | <http://localhost:8083> |
| Kafka UI | Topicok és üzenetek megtekintése | <http://localhost:8081> |

A konténerek egymás között nem a host portokat használják.

Például az Adminer a MySQL-t ezen a címen éri el:

```text
mysql:3306
```

Az Adminer maga a konténeren belül továbbra is a `8080` porton fut:

```text
host:      localhost:18080
                     ↓
container: adminer:8080
```

---

## 4. Adminer megnyitása

Nyisd meg:

<http://localhost:18080>

Belépés:

| Mező | Érték |
| --- | --- |
| System | `MySQL` |
| Server | `mysql` |
| Username | `bank` |
| Password | `bank_lab_password` |
| Database | `bank` |

A `Server` mezőbe **`mysql`** kerül, mert az Adminer a Compose belső hálózatán ezen a service néven éri el a MySQL konténert.

Belépés után két fontos táblát látsz:

```text
ACCOUNT
PAYMENT
```

Kezdetben például ezek az adatok vannak benne:

```text
ACCOUNT
1001  Kiss Anna   150000.00 HUF
1002  Nagy Peter  250000.00 HUF

PAYMENT
1  1001  12500.00 HUF  COMPLETED
2  1002   3400.00 HUF  PENDING
```

---

## 5. Kafka UI megnyitása

Nyisd meg:

<http://localhost:8081>

A cluster neve:

```text
bank-lab
```

A Debezium connector regisztrálása előtt a két CDC topic még nem feltétlenül létezik.

Később ezeket fogod keresni:

```text
bank-cdc.bank.ACCOUNT
bank-cdc.bank.PAYMENT
```

---

## 6. A Debezium connector konfigurációja

A konfiguráció itt található:

```text
connectors/bank-mysql.json
```

A fontosabb részek:

```json
"connector.class": "io.debezium.connector.mysql.MySqlConnector"
```

MySQL Debezium connectort használunk.

```json
"database.hostname": "mysql"
```

A `mysql` service-t figyeli.

```json
"database.user": "debezium"
```

A connector külön technikai DB usert használ.

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

Ezért lesz például:

```text
bank-cdc.bank.PAYMENT
```

```json
"snapshot.mode": "initial"
```

Első induláskor a Debezium a már meglévő rekordokat is beolvassa.

Utána folyamatosan a MySQL binlogból dolgozik.

---

## 7. A connector regisztrálása

A környezet elindítása még nem hozza létre automatikusan a `bank-mysql` connectort.

PowerShellből:

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://localhost:8083/connectors `
  -ContentType 'application/json' `
  -Body (Get-Content -Raw .\connectors\bank-mysql.json)
```

Ez történik:

```text
bank-mysql.json
      ↓ HTTP POST
Kafka Connect REST API
      ↓
bank-mysql Debezium connector
```

Sikeres regisztráció után a válaszban megjelenik:

```text
bank-mysql
```

> Ha a connector már létezik, az újabb POST `409 Conflict` hibát adhat. Ilyenkor nem kell újra létrehozni.

---

## 8. Connector státusz ellenőrzése

PowerShell:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/bank-mysql/status |
  ConvertTo-Json -Depth 10
```

Ezt keresd:

```text
connector.state = RUNNING
tasks[0].state   = RUNNING
```

Példa:

```json
{
  "name": "bank-mysql",
  "connector": {
    "state": "RUNNING"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING"
    }
  ],
  "type": "source"
}
```

A `type = source` azt jelzi, hogy ez egy source connector: a MySQL-ből olvas változásokat.

---

## 9. A kezdeti snapshot megfigyelése

Kafka UI:

```text
Topics
  → bank-cdc.bank.PAYMENT
  → Messages
```

Állítsd a kezdőpozíciót `Beginning / earliest` értékre.

Az induláskor már létező sorok ilyen eventként jelennek meg:

```json
{
  "before": null,
  "after": {
    "id": 1,
    "account_id": 1001,
    "amount": "12500.00",
    "currency": "HUF",
    "status": "COMPLETED"
  },
  "source": {
    "db": "bank",
    "table": "PAYMENT",
    "snapshot": "first_in_data_collection"
  },
  "op": "r"
}
```

A lényeg:

```text
op = r        snapshot read
before = null nincs korábbi állapot
after         a beolvasott sor
```

Ez **nem INSERT**.

A sor már létezett a Debezium indulása előtt, a connector csak felvette a kezdeti állapotot.

---

## 10. Valódi UPDATE kipróbálása

Adminerben menj az SQL parancs oldalra, és futtasd:

```sql
UPDATE PAYMENT
SET
    status = 'SETTLED',
    amount = 13000.00,
    updated_at = NOW(3)
WHERE id = 1;
```

Ezután menj vissza:

```text
Kafka UI
  → Topics
  → bank-cdc.bank.PAYMENT
  → Messages
```

Az új eseményben ezt kell látnod:

```text
op = u
snapshot = false
```

Például:

```json
{
  "before": {
    "id": 1,
    "amount": "12500.00",
    "status": "COMPLETED"
  },
  "after": {
    "id": 1,
    "amount": "13000.00",
    "status": "SETTLED"
  },
  "op": "u"
}
```

A folyamat:

```text
SQL UPDATE
   ↓
MySQL COMMIT
   ↓
MySQL binlog
   ↓
Debezium
   ↓
Kafka
```

A Debezium **nem az Adminert figyeli**.

Ugyanilyen CDC event keletkezne akkor is, ha a módosítást Java alkalmazás, batch vagy másik SQL kliens végezné.

---

## 11. A `source` blokk értelmezése

Streaming eventnél például:

```json
"source": {
  "connector": "mysql",
  "db": "bank",
  "table": "PAYMENT",
  "snapshot": "false",
  "server_id": 1,
  "gtid": "...",
  "file": "mysql-bin.000003",
  "pos": 442
}
```

Röviden:

| Mező | Jelentés |
| --- | --- |
| `db` | Source adatbázis |
| `table` | Source tábla |
| `snapshot` | Snapshot vagy normál streaming event |
| `file` | MySQL binlog fájl |
| `pos` | Pozíció a binlogban |
| `server_id` | A MySQL server azonosítója |
| `gtid` | MySQL tranzakcióazonosító |

MySQL-ben a `binlog file + position + GTID` segít megmondani, hol járunk a változásfolyamban.

Oracle CDC-nél hasonló szerepe van a redo / SCN / logpozíció gondolkodásnak.

---

## 12. INSERT kipróbálása

Adminerben:

```sql
INSERT INTO PAYMENT
  (id, account_id, amount, currency, status, created_at, updated_at)
VALUES
  (9001, 1001, 12.34, 'HUF', 'NEW', NOW(3), NOW(3));
```

Kafka UI-ban:

```text
op = c
before = null
after = új sor
```

A `c` jelentése:

```text
create / INSERT
```

---

## 13. DELETE és tombstone kipróbálása

Adminerben:

```sql
DELETE FROM PAYMENT
WHERE id = 9001;
```

A Kafka topicban először egy Debezium DELETE event jelenik meg:

```text
op = d
before = törölt sor
after = null
```

Utána egy külön Kafka tombstone:

```text
key   = ugyanaz a kulcs
value = null
```

A DELETE event és a tombstone **két külön Kafka record**.

---

## 14. Debezium `op` értékek

| `op` | Jelentés |
| --- | --- |
| `r` | Snapshot read |
| `c` | Create / INSERT |
| `u` | UPDATE |
| `d` | DELETE |

---

## 15. Hasznos Kafka Connect REST hívások

Connectorok listája:

```powershell
Invoke-RestMethod http://localhost:8083/connectors
```

Connector státusz:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/bank-mysql/status |
  ConvertTo-Json -Depth 10
```

Connector konfiguráció:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/bank-mysql/config |
  ConvertTo-Json -Depth 10
```

Mentett source offset:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/bank-mysql/offsets |
  ConvertTo-Json -Depth 20
```

---

## 16. Logok

### Docker

Kafka Connect:

```powershell
docker compose logs --tail=150 connect
```

MySQL:

```powershell
docker compose logs --tail=100 mysql
```

### Podman

Kafka Connect:

```powershell
podman compose logs --tail=150 connect
```

MySQL:

```powershell
podman compose logs --tail=100 mysql
```

---

## 17. Egyszerű restart / recovery próba

A cél annak megfigyelése, hogy a Debezium nem csak az aktuális táblaállapotot kérdezi le, hanem a változásnaplóból folytatja a feldolgozást.

### 1. Állítsd le csak a Kafka Connect service-t

Docker:

```powershell
docker compose stop connect
```

Podman:

```powershell
podman compose stop connect
```

MySQL és Kafka maradjon futva.

### 2. Adminerben módosíts egy rekordot

Például:

```sql
UPDATE PAYMENT
SET status = 'RECOVERED',
    updated_at = NOW(3)
WHERE id = 2;
```

A MySQL-ben a változás megtörténik, de a Debezium most nem fut.

### 3. Indítsd vissza a Connectet

Docker:

```powershell
docker compose start connect
```

Podman:

```powershell
podman compose start connect
```

### 4. Ellenőrizd a státuszt

```powershell
Invoke-RestMethod http://localhost:8083/connectors/bank-mysql/status |
  ConvertTo-Json -Depth 10
```

### 5. Nézd meg Kafka UI-ban az új eseményt

A Debezium a mentett source offset alapján folytatja a binlog olvasását, ezért a Connect leállása alatt történt változásnak is meg kell jelennie.

```text
Connect áll
   ↓
MySQL tovább változik
   ↓
változás bekerül a binlogba
   ↓
Connect újraindul
   ↓
Debezium folytatja a mentett pozíciótól
   ↓
event megjelenik Kafkában
```

---

## 18. Környezet leállítása

### Docker

```powershell
docker compose down
```

### Podman

```powershell
podman compose down
```

Ez a volume-okat megtartja.

A következő induláskor az adatbázis és a Kafka állapota megmarad.

---

## 19. Teljes tiszta újrakezdés

Csak akkor használd, ha tényleg mindent törölni akarsz:

### Docker

```powershell
docker compose down -v
docker compose up -d
```

### Podman

```powershell
podman compose down -v
podman compose up -d
```

A `-v` törli többek között:

- a MySQL adatokat,
- a MySQL binlogot,
- a Kafka topicokat,
- a Kafka Connect offseteket,
- a connector korábbi állapotát.

Ezután a connector regisztrációját újra el kell végezni, és új kezdeti snapshot készül.

---

## 20. Fontos fájlok

```text
debezium-cdc/
├── docker-compose.yml
├── connectors/
│   └── bank-mysql.json
├── mysql/
│   └── init.sql
├── scripts/
├── tests/
└── README.md
```

### `docker-compose.yml`

A local runtime:

```text
MySQL
Kafka
Kafka Connect + Debezium
Kafka UI
Adminer
```

### `mysql/init.sql`

Létrehozza:

```text
bank adatbázis
ACCOUNT tábla
PAYMENT tábla
kezdeti rekordok
Debezium technikai user
```

### `connectors/bank-mysql.json`

Megmondja a Debeziumnak:

```text
melyik MySQL-t figyelje
melyik adatbázist figyelje
melyik táblákat figyelje
milyen topic prefixet használjon
készítsen-e initial snapshotot
```

---

## 21. IBM CDC és Debezium – mentális megfeleltetés

Ez nem 1:1 termékmegfeleltetés, csak segítség a fogalmak összekötéséhez.

| IBM CDC | Debezium |
| --- | --- |
| Source CDC Engine | Debezium source connector |
| Subscription | Connector konfiguráció / connector instance |
| Table Mapping | `table.include.list` és további filter/config |
| Bookmark | Source offset |
| Refresh | Snapshot |
| Mirroring | Streaming |

A Debezium Kafka Connectes modelljében nincs külön IBM CDC-szerű Kafka target engine. A source connector a Kafka Connect runtime-on keresztül Kafka topicokba ír.

---

## 22. Röviden: mit láttunk?

```text
1. MySQL-ben már léteztek ACCOUNT és PAYMENT sorok
2. Elindítottuk a local környezetet
3. Regisztráltuk a bank-mysql Debezium connectort
4. Debezium initial snapshotot készített
5. A meglévő sorok op=r eventként megjelentek Kafkában
6. Adminerben UPDATE-et hajtottunk végre
7. A változás bekerült a MySQL binlogba
8. Debezium kiolvasta
9. Kafka topicban op=u event jelent meg
10. before = régi állapot
11. after = új állapot
```

Ez a CDC alapfolyam:

```text
adatbázis-változás
      ↓
transaction log / binlog
      ↓
Debezium
      ↓
Kafka event
```
