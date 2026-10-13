# Debezium CDC banki gyakorlólab

Ebben a projektben egy MySQL-adatbázis `ACCOUNT` és `PAYMENT` tábláinak változásait nézheted meg Kafka-üzenetként. A Debezium először beolvassa a már létező sorokat (**snapshot**), majd a MySQL binlogból folyamatosan követi az új változásokat (**streaming**). Adminerben SQL-t futtathatsz, a Kafka UI-ban pedig megnézheted a hozzájuk tartozó eseményeket.

```text
MySQL → Debezium MySQL connector a Kafka Connectben → Kafka → Kafka UI
  ↑
Adminer
```

Ez helyi fejlesztői lab, ismert gyakorlójelszavakkal és titkosítás nélkül. A közzétett portok csak a laptopod `127.0.0.1` címén érhetők el. Dockeres környezeten belül a konténerek szolgáltatásnévvel kommunikálnak. Oracle, Spring Boot, Avro, Schema Registry, Kubernetes és SSL/SASL nem szükséges hozzá. A Kafka KRaft módban fut, ZooKeeper nélkül.

## Előfeltételek

- Windows alatt Docker Desktop, Linux-konténerek és a WSL2 backend; Linuxon Docker Engine.
- Docker Compose V2: a `docker compose version` parancsnak működnie kell.
- Futó Docker daemon: a `docker info` parancs ne jelezzen kapcsolódási hibát.
- Körülbelül 6 GB, Docker számára rendelkezésre álló memória és több GB szabad lemezterület.
- Szabad helyi portok: `3306`, `8080`, `8081`, `8083`, `9092`.
- Internet az első induláshoz, hogy Docker letölthesse az image-eket.
- Windows PowerShell 5.1 vagy PowerShell 7. A bash változatokhoz bash és curl kell.

A parancsokat a projekt gyökérkönyvtárában futtasd, ahol a `docker-compose.yml` található. A `powershell -ExecutionPolicy Bypass -File ...` csak az adott folyamatra vonatkozik; nem változtatja meg tartósan a gép execution policy beállítását. PowerShell 7 használatakor a `powershell` helyett `pwsh` is használható.

## Fájlok és szolgáltatások

| Fájl | Feladat |
| --- | --- |
| `docker-compose.yml` | MySQL, Kafka, Kafka Connect/Debezium, Kafka UI és Adminer; hálózat, portok, állapotellenőrzések és tartós volume-ok. |
| `mysql/init.sql` | A `bank` adatbázis, a CDC-felhasználó, a banki táblák és a kezdeti rekordok létrehozása az első MySQL-induláskor. |
| `connectors/bank-mysql.json` | A `bank-mysql` connector regisztrációs JSON-ja; a MySQL kapcsolata, a két figyelt tábla, a topicnevek és a snapshot beállítása. |
| `scripts/start.ps1`, `scripts/start.sh` | A Compose-szolgáltatások elindítása és a készenlét ellenőrzése. |
| `scripts/register-connector.ps1`, `scripts/register-connector.sh` | A connector létrehozása vagy a már létező konfiguráció frissítése. |
| `scripts/status.ps1`, `scripts/status.sh` | Connector- és taskstátusz lekérdezése. |
| `scripts/demo.ps1`, `scripts/demo.sh` | Egy `PAYMENT` sor INSERT → UPDATE → DELETE bemutatója. |
| `scripts/recovery-test.ps1`, `scripts/recovery-test.sh` | A Connect leállítása, közben SQL-módosítások, majd újraindítás és felzárkózás ellenőrzése. |
| `scripts/common.ps1`, `scripts/common.sh` | A scriptek közös kapcsolódási, várakozási és Kafka-olvasási segédfüggvényei. |
| `tests/verify_cdc.py` | Opcionális, automatikus ellenőrzés valódi Docker-szolgáltatások és Kafka-üzenetek alapján; Python standard libraryvel. |
| `artifacts/` | Az automatikus teszt JSON-bizonyítékai, például státuszok, események és offsetek. |
| `TEST-RESULTS.md` | A projekt elkészítésekor végzett tényleges ellenőrzések eredménye és az esetleges környezeti korlátok. |
| `README.md` | A kézi megfigyelés, az események értelmezése és a helyi futtatás útmutatója. |

| Szolgáltatás | Böngésző/helyi kliens | Konténerek közötti cím |
| --- | --- | --- |
| Adminer | <http://localhost:8080> | `adminer:8080` |
| Kafka UI | <http://localhost:8081> | `kafka-ui:8080` |
| Kafka Connect REST | <http://localhost:8083> | `connect:8083` |
| MySQL | `localhost:3306` | `mysql:3306` |
| Kafka broker | `localhost:9092` | `kafka:9092` |

Az `ACCOUNT` számlákat, a `PAYMENT` pedig számlához tartozó fizetéseket tartalmaz. A fizetés `account_id` mezője az `ACCOUNT.id` mezőre mutató idegen kulcs. A kezdeti számlaazonosítók `1001` és `1002`, a kezdeti fizetésazonosítók `1` és `2`. A demókhoz a `9001`, `9002` és `9003` azonosítókat használjuk; ezeket hagyd szabadon saját adataidhoz.

## 1. A teljes környezet elindítása

Windows PowerShellből:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1
docker compose ps
```

Linux/macOS alatt:

```bash
bash scripts/start.sh
docker compose ps
```

Az első image-letöltés és indulás néhány percig tarthat. A MySQL-init csak **üres MySQL-volume esetén** fut le; későbbi indításkor a már létrehozott adatbázis marad meg. Az indítás után a connector regisztrációja még külön lépés, hogy meg tudd figyelni a kezdeti snapshotot.

Kézi indításkor a megfelelő parancs:

```bash
docker compose up -d --wait
```

## 2. Adminer megnyitása

Nyisd meg: <http://localhost:8080>.

| Bejelentkezési mező | Érték |
| --- | --- |
| Rendszer / System | `MySQL` |
| Szerver / Server | `mysql` |
| Felhasználó / Username | `bank` |
| Jelszó / Password | `bank_lab_password` |
| Adatbázis / Database | `bank` |

A szerver mezőbe **`mysql`** kerül: az Adminer a saját konténeréből kapcsolódik. Válaszd ki az `ACCOUNT` és `PAYMENT` táblát, és nézd meg a kezdeti sorokat. Az **SQL parancs / SQL command** oldalon a README SQL-példáit is lefuttathatod.

A connector külön `debezium` felhasználót használ, `debezium_lab_password` gyakorlójelszóval. A `bank` felhasználóval a banki adatokat nézheted és módosíthatod; a root gyakorlójelszót a Compose a MySQL kezdeti létrehozásához adja meg.

## 3. Kafka UI megnyitása

Nyisd meg: <http://localhost:8081>. Válaszd a helyi Kafka-klasztert, majd a **Topics** menüpontot. A connector regisztrálása előtt a két banki topic még nem feltétlenül létezik.

A connector és taskjai a **Kafka Connect** nézetben is láthatók. A REST API-val a következő lépésekben külön is ellenőrizzük őket.

## 4. A Debezium connector regisztrálása

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\register-connector.ps1
```

Bash:

```bash
bash scripts/register-connector.sh
```

A script a `connectors/bank-mysql.json` fájlból dolgozik. A connector neve `bank-mysql`; csak a `bank.ACCOUNT` és `bank.PAYMENT` táblát figyeli. A script újrafuttatható: a már meglévő connector konfigurációját frissíti, nem hoz létre új nevű connectort. A regisztráció sikeres REST-válasza után a connector indulása és a snapshot még aszinkron történik.

Kézi, első regisztráció PowerShellben:

```powershell
$connector = Get-Content -Raw .\connectors\bank-mysql.json
Invoke-RestMethod -Method Post -Uri http://localhost:8083/connectors `
  -ContentType 'application/json' -Body $connector
```

Kézi, első regisztráció bashből:

```bash
curl --fail-with-body -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data-binary @connectors/bank-mysql.json
```

A kézi `POST` már létező név esetén `409 Conflict` választ ad; ilyenkor használd az újrafuttatható regisztrációs scriptet.

## 5. A connector státuszának ellenőrzése

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\status.ps1
```

Bash:

```bash
bash scripts/status.sh
```

Közvetlen lekérdezés PowerShellben:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/bank-mysql/status |
  ConvertTo-Json -Depth 10
```

A connector és az egyetlen task állapota is **`RUNNING`** legyen:

```json
{
  "name": "bank-mysql",
  "connector": { "state": "RUNNING" },
  "tasks": [{ "id": 0, "state": "RUNNING" }]
}
```

Ez rövidített példa; a valódi válasz további mezőket is tartalmaz. A `RUNNING` állapot önmagában még nem bizonyítja a snapshot végét. A kezdeti rekordok megjelenése és a streamingesemények feldolgozása adja a használható ellenőrzést.

Hiba esetén nézd meg a státuszban a `trace` mezőt, illetve:

```bash
docker compose logs --tail=150 connect
docker compose logs --tail=100 mysql
```

## 6. A snapshot során keletkező események

A Kafka UI **Topics** listájában keresd ezt a két topicot:

- `bank-cdc.bank.ACCOUNT`
- `bank-cdc.bank.PAYMENT`

Nyisd meg valamelyiket, válaszd a **Messages** fület, és a kezdőpozíciót állítsd **Beginning / earliest** értékre. Ha szükséges, indíts új keresést vagy frissítsd a listát. Az újabb üzenetekhez kapcsolódó nézetbeállítás nem mutatja a korábban létrejött snapshotüzeneteket.

Nyisd ki egy kezdeti sor üzenetét. Az érték JSON, az `op` mező **`r`** (*read*), az `after` pedig a beolvasott adatbázissor. Snapshotban a `before` általában `null`. A `source.snapshot` mező a snapshot állapotáról is információt ad: `first`, `true`, `last_in_data_collection` és `last` értékkel is találkozhatsz. Ezek a snapshot első sorát, köztes sorát, egy tábla utolsó sorát, illetve az egész snapshot utolsó sorát jelölhetik. A snapshotesemény felismerésének biztos támpontja az **`op=r`**; nem csak a `source.snapshot="true"` értéket kell keresni.

```json
{
  "before": null,
  "after": { "id": 1, "account_id": 1001, "amount": "12500.00" },
  "source": { "db": "bank", "table": "PAYMENT", "snapshot": "true" },
  "op": "r",
  "ts_ms": 1760000000000
}
```

Ez szemléltető, rövidített példa, nem a kezdeti rekord teljes, pontos másolata. A project JSON-converterei nem csomagolják az adatot `schema`/`payload` mezőkbe: a `before`, `after`, `source`, `op` és `ts_ms` közvetlenül az esemény tetején található.

## 7. INSERT, UPDATE és DELETE végrehajtása

A következő SQL-parancsokat Adminer **SQL command** oldalán futtasd **egyenként**. Minden művelet után nézd meg a Kafka UI-ban a `PAYMENT` topicot. A sor `9001` azonosítója segít a sok üzenet között megtalálni a megfelelő eseményt.

**INSERT:**

```sql
INSERT INTO PAYMENT
  (id, account_id, amount, currency, status, created_at, updated_at)
VALUES
  (9001, 1001, 12.34, 'HUF', 'NEW', NOW(3), NOW(3));
```

**UPDATE:**

```sql
UPDATE PAYMENT
SET amount = 56.78, status = 'SETTLED', updated_at = NOW(3)
WHERE id = 9001;
```

**DELETE:**

```sql
DELETE FROM PAYMENT WHERE id = 9001;
```

Mindegyik önálló, commitolt változásként fut az Adminer alapértelmezett autocommit működésével. Nyitott tranzakcióban az események a commit után jelennek meg.

Az ugyanezt végrehajtó Windows-demo:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\demo.ps1
```

Bashből:

```bash
bash scripts/demo.sh
```

A script egymás után hajtja végre a fizetés INSERT, UPDATE és DELETE műveletét. A `9001` demoazonosítóhoz tartozó esetleges korábbi sor takarítása további DELETE-eseményt is eredményezhet; ismételt futtatáskor az újabb Kafka-offseteket vizsgáld. Sikeres futtatás után maga a `9001` sor már nem szerepel a MySQL-táblában, az eseményei viszont megmaradnak a Kafka-topicban a megőrzési szabályok szerint.

## 8. A módosítások megkeresése a Kafka UI-ban

Nyisd meg a `bank-cdc.bank.PAYMENT` topic **Messages** nézetét, és frissítsd a beolvasást. A kulcs JSON-ként az elsődleges kulcsot tartalmazza, például:

```json
{ "id": 9001 }
```

Keresd ezt a kulcsot, és hasonlítsd össze az egyre növekvő Kafka-offseteket. A `9001` sorhoz a három adatváltozási esemény után egy negyedik, `null` értékű tombstone-üzenet is érkezik:

| Művelet | `op` | `before` | `after` |
| --- | --- | --- | --- |
| Snapshot | `r` | `null` | A beolvasott sor |
| INSERT | `c` | `null` | Az új sor |
| UPDATE | `u` | A módosítás előtti sor | A módosítás utáni sor |
| DELETE | `d` | A törölt sor | `null` |
| Kafka tombstone | Nincs eseményboríték | Nincs | Az üzenet teljes értéke `null` |

A DELETE és a tombstone **két külön Kafka-üzenet**, azonos kulccsal. A törlési eseményben van `op=d`, `before` és `after=null`; a tombstone teljes Kafka-értéke `null`, ezért nincs benne `op` sem. Kompaktált topicok esetén a tombstone jelzi a korábbi kulcs eltávolítását. A lab adatváltozási topicjaiban a DELETE és a tombstone egyaránt megfigyelhető a megőrzési időn belül.

Ha a tombstone nem látszik, kapcsold ki a csak nem üres értékeket mutató szűrést, és a kulcs/offset alapján keresd. Egy olyan tartalomszűrő, amely például az `op` mezőt vagy az értékben az `id` mezőt keresi, elrejtheti a tombstone-t, mert annak nincs JSON-értéke. Egyes UI-nézetek `NULL` feliratként vagy üres értékként jelenítik meg.

## 9. A Debezium-mezők értelmezése

| Mező | Jelentés |
| --- | --- |
| `before` | A változás előtti adatbázissor. INSERT és snapshot esetén `null`. |
| `after` | A változás utáni adatbázissor. DELETE esetén `null`. |
| `source` | A forrás metaadatai: adatbázis, tábla, binlogpozíció és snapshotjelző; a MySQL-esemény ideje is itt található. |
| `op` | `r` = snapshot read, `c` = create/INSERT, `u` = UPDATE, `d` = DELETE. |
| `ts_ms` | Az esemény connector általi feldolgozásának időbélyege, Unix epoch milliszekundumban. |
| Kafka key | Az adatbázissor elsődleges kulcsa; itt a sor `id` értéke. |
| Kafka offset | Az üzenet pozíciója egy Kafka-topic adott partíciójában. |

A MySQL `binlog_format=ROW` és `binlog_row_image=FULL` beállítással fut. Ezért az UPDATE `before` és `after` mezőjében a teljes sor látható, nem csak a megváltozott oszlopok. A pénzösszegek stringként jelennek meg, például `"55.75"`, hogy a tizedes pontosság megmaradjon.

A példabeli frissítés rövidítve:

```json
{
  "before": { "id": 9001, "amount": "12.34", "status": "NEW" },
  "after": { "id": 9001, "amount": "56.78", "status": "SETTLED" },
  "source": { "db": "bank", "table": "PAYMENT", "snapshot": "false" },
  "op": "u",
  "ts_ms": 1760000001000
}
```

A `ts_ms` nem tranzakciós sorszám, és nem garantál globális sorrendet két topic között. Kafka partíción belüli offsetekkel ellenőrizheted a beolvasott üzenetek sorrendjét; azonos kulcs eseményei ugyanabba a partícióba kerülnek. A connector által tárolt **forrásoffset** és a topic **Kafka-offsetje** eltérő fogalom: előbbi azt írja le, meddig jutott a connector a MySQL naplójában, utóbbi a Kafka-üzenet helyét jelöli.

## 10. Restart és offset recovery teszt

A teszt előtt legyen a connector és a task is `RUNNING`, és várd meg, hogy a kezdeti snapshot megjelenjen a két topicban.

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\recovery-test.ps1
```

Bash:

```bash
bash scripts/recovery-test.sh
```

A script a következő folyamatot mutatja be:

1. Működő connector mellett létrehozza a `9002` fizetést, és hagy időt a feldolgozásra és offsetmentésre.
2. **Csak a `connect` konténert állítja le**. MySQL, Kafka, Adminer és Kafka UI tovább fut.
3. Leállított Connect mellett módosítja a `9002` fizetést, és létrehozza, majd törli a `9003` fizetést. Ezek a változások MySQL-ben már megtörténnek, miközben nincs futó Debezium-task.
4. Újraindítja a `connect` szolgáltatást, és megvárja a `RUNNING` státuszt.
5. A Debezium a mentett forrásoffsettől folytatja a binlog olvasását, és feldolgozza a kiesés közben történt változásokat.

Kafka UI-ban a `bank-cdc.bank.PAYMENT` topic újabb offsetjein keresd:

- `id=9002`, `op=c`: a leállítás előtt létrehozott sor.
- `id=9002`, `op=u`: a kiesés közben végrehajtott frissítés, benne a régi és az új sorral.
- `id=9003`, `op=c`, majd `op=d`, majd ugyanennek a kulcsnak a tombstone-ja: a kiesés közben létrehozott és törölt sor.

A recovery-script a Kafka-üzeneteket be is olvassa és ellenőrzi. A sikeres futás végén `PASS` eredményt ír ki; hiányzó UPDATE/INSERT/DELETE/tombstone vagy a tesztsorokra érkező új snapshot esetén hibával áll le. A Kafka UI-ban ugyanennek az eredménynek a kézi megfigyelését végezheted el. A `9002` sor `RECOVERED` állapotban megmarad, a `9003` sor törlődik.

Különösen tanulságos a `9003`: újraindításkor már nincs a MySQL-táblában, mégis megkapod a létrehozási és törlési eseményét, mert Debezium a binlogból olvassa vissza a közben történt változásokat. Ez nem a pillanatnyi táblaállapot újbóli lekérdezése.

Az új események streamingesemények, nem `op=r` snapshotok. A korábbi `op=r` üzenetek továbbra is ott lehetnek a topicban; az új Kafka-offseteket és a demoazonosítókat nézd. A normál Connect-restart nem indít új teljes snapshotot, mert a mentett offset és a sémaelőtörténet megmarad.

Az offsetek REST-lekérdezése PowerShellből:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/bank-mysql/offsets |
  ConvertTo-Json -Depth 20
```

Bashből:

```bash
curl --fail-with-body http://localhost:8083/connectors/bank-mysql/offsets
```

A forrásoffsetben a MySQL binlog fájlja és pozíciója, valamint egyéb állapotmezők szerepelhetnek. Egy normál, leállítás előtti offsetmentés után a connector innen tud folytatni. Az offsetmentés periodikus; váratlan összeomlás után néhány már elküldött esemény újra megjelenhet. A feldolgozás alapértelmezett viselkedése **at least once**, ezért a lab nem ígér teljesen duplikációmentes helyreállást.

A MySQL-binlog is megmarad a MySQL-volume-ban. Ha a szükséges binlogot már törölték vagy lejárt, a forrásoffset önmagában nem elég a felzárkózáshoz. Ebben a rövid helyi tesztben a binlog-megőrzés bőven elegendő.

## Opcionális teljes automatikus ellenőrzés

Az elindított és regisztrált labot Python 3.10 vagy újabb verzióval is ellenőrizheted; ehhez nincs pip-csomagtelepítés. Windows alatt, telepített Python esetén:

```powershell
py -3 .\tests\verify_cdc.py
```

Linux/macOS alatt:

```bash
python3 tests/verify_cdc.py
```

A teszt megvizsgálja a connector és task állapotát, az Adminer és Kafka UI elérhetőségét, mindkét tábla kezdeti snapshotját, a fizetés INSERT/UPDATE/DELETE/tombstone eseményeit és a Connect-only restart utáni offset recoveryt. A `9901`, `9902`, `9903` PAYMENT-azonosítókat használja és siker esetén eltakarítja őket; ezeket is hagyd szabadon. Nem töröl volume-okat. A korábbi snapshotüzeneteknek még elérhetőnek kell lenniük a topicban.

Az eredmény az `artifacts/validation.json` fájlba kerül, a konkrét üzenetekkel és offsetekkel együtt külön bizonyítékfájlok is készülnek. Siker esetén a script nulla kilépési kódot és `PASS` sorokat ad, hiba esetén nem nulla kóddal tér vissza. A teszt saját maga is leállítja és újraindítja a Connectet, ezért futása alatt ne végezz párhuzamos SQL-demót vagy másik restarttesztet.

A projekt elkészítésekor ténylegesen elvégzett ellenőrzéseket és azok futtatási körülményeit a [TEST-RESULTS.md](TEST-RESULTS.md) dokumentálja.

## Tartós állapot, leállítás és tiszta újrakezdés

A `mysql_data` volume tárolja az adatbázist és a binlogot. A `kafka_data` volume-ban a topicok mellett a Kafka Connect konfigurációs, offset- és státusztopicjai, illetve a Debezium sémaelőtörténete is megmarad. Emiatt a konténerek újraindítása után a connector ugyanazzal a konfigurációval és állapottal folytathatja a munkát.

A szolgáltatások leállítása az adatok megőrzésével:

```bash
docker compose down
```

Újraindítás:

```bash
docker compose up -d --wait
```

Ilyenkor egy már regisztrált connector is visszatér; nem kell újból létrehozni.

**Teljes, adatvesztéssel járó újrakezdés** csak akkor, ha tényleg új snapshotot és üres Kafka-állapotot szeretnél:

```bash
docker compose down -v
docker compose up -d --wait
```

Ez törli a lab MySQL-adatait, Kafka-üzeneteit és connectoroffsetjeit. Ezután ismét futtasd a regisztrációs scriptet. A connector törlése vagy a regisztrációs script újrafuttatása önmagában nem biztosít tiszta snapshot-resetet; az offsetek és topicok állapotát is figyelembe kell venni.

## Debezium és IBM CDC: mentális megfeleltetés

| IBM CDC fogalom | Nagyjából kapcsolódó Debezium-fogalom |
| --- | --- |
| Source CDC Engine | Debezium connector, amely a forrásadatbázis naplóját olvassa |
| Subscription | Connector konfigurációja |
| Table Mapping | `table.include.list`, illetve egyéb táblaszűrési és adatátalakítási konfiguráció |
| Bookmark | A connector mentett forrásoffsetje |
| Refresh | Kezdeti snapshot |
| Mirroring | Folyamatos streaming a változásnaplóból |

Ez segít a már ismert fogalmakhoz kapcsolni a lab működését, de **nem 1:1 funkcionális megfeleltetés**. Például ez a Debezium-lab Kafka-topicokra publikál eseményeket; önmagában nem alkalmazza a módosításokat egy céladatbázis tábláin.

## Gyakori hibák

- **A Docker daemon nem érhető el:** indítsd el Docker Desktopot, várd meg az engine indulását, és ellenőrizd a `docker info` kimenetét.
- **Foglalt port:** állítsd le az adott portot használó helyi szolgáltatást, vagy módosítsd a Compose hostportját és a hozzá tartozó böngésző-/scriptcímet együtt.
- **Adminer nem csatlakozik:** az Adminer szervermezője `mysql`, az adatbázis `bank`; ellenőrizd a MySQL állapotát a `docker compose ps` paranccsal.
- **Connector `FAILED`:** a státusz `trace` mezője és a Connect-log megmondja az okot. A MySQLnek egészségesen kell futnia a connector regisztrációja előtt.
- **Nem látok snapshotot:** a Kafka UI üzenetnézete legyen `Beginning/earliest`, és a megfelelő `bank-cdc.bank.*` topicot nézd. Korábban használt volume mellett a connector normálisan streaminggel folytathatja a munkát.
- **Nem látok friss módosítást:** commitold a tranzakciót, ellenőrizd a task `RUNNING` állapotát, majd indíts új beolvasást a Kafka UI-ban.
- **UPDATE-ben hiányos a `before`:** ellenőrizd, hogy a futó MySQL `binlog_row_image` értéke `FULL`; a projekt ezt állítja be.
- **Tombstone nem látszik:** töröld az értéktartalomra épülő szűrőt, és a kulcsot, partíciót, illetve offsetet nézd.
- **Init SQL módosítása nem változtatja az adatbázist:** meglévő MySQL-volume esetén az init nem fut újra. A teljes reset törli a labadatokat; csak tudatos újrakezdéshez használd.

Hasznos ellenőrző SQL Adminerben:

```sql
SHOW VARIABLES WHERE Variable_name IN
  ('log_bin', 'binlog_format', 'binlog_row_image', 'server_id');
SELECT * FROM ACCOUNT ORDER BY id;
SELECT * FROM PAYMENT ORDER BY id;
```

A CDC-lab működését akkor igazoltad helyben, ha láttad a két tábla kezdeti `op=r` eseményeit, a demo `c/u/d` és tombstone üzeneteit, valamint a Connect-kiesés közben végrehajtott változások utólagos feldolgozását, új snapshot nélkül.
