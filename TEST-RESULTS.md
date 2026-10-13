# Tényleges futtatási eredmények

Ellenőrzés dátuma: **2026-10-13**. A labot valóban elindítottuk Dockerrel egy Linux/amd64 felhőkörnyezetben: Docker Engine 28.4.0, Docker Compose 2.40.3, Python 3.12.14, PowerShell 7.4.6.

| Ellenőrzés | Eredmény |
| --- | --- |
| Mind az öt Compose-szolgáltatás indulása és healthcheckje | Sikeres |
| `bank-mysql` connector és task indulása | Mindkettő `RUNNING` |
| `ACCOUNT` kezdeti snapshot | 2 rekord, `op=r`, kulcsok 1001 és 1002 |
| `PAYMENT` kezdeti snapshot | 2 rekord, `op=r`, kulcsok 1 és 2 |
| PAYMENT INSERT | `op=c`, `before=null`, az új sor az `after` mezőben |
| PAYMENT UPDATE | `op=u`, teljes régi és új sor, megváltozott összeg és státusz |
| PAYMENT DELETE | `op=d`, teljes `before`, `after=null` |
| Kafka tombstone | Azonos kulcs, valódi null Kafka-érték a DELETE után |
| Connect-only leállítás | MySQL, Kafka, Adminer és Kafka UI tovább futott; a topic vég-offsetje a kiesés alatt nem változott |
| Offset recovery | Kiesés alatti UPDATE, INSERT, DELETE és tombstone megérkezett újraindítás után, új snapshot nélkül |
| Mentett forrásoffset | REST-lekérdezéssel előtte és utána ellenőrizve; binlog-pozíció 1380 → 2494, a GTID-állapot is előrelépett |
| PowerShell start / register / status / demo / recovery | Mind végigfutott, nulla kilépési kóddal |
| Bash start / register / status / demo / recovery | Mind végigfutott, nulla kilépési kóddal |
| Mindkét demo tényleges Kafka-eseményei | A 9001 kulcsra kétszer `c → u → d → null` |
| Regisztráció újrafuttatása | A meglévő konfiguráció frissült, az offsetek megmaradtak |
| Adminer | Sikeres HTTP-bejelentkezés `bank` felhasználóval; ACCOUNT és PAYMENT látható |
| Kafka UI | `bank-lab` online; az üzenet-API CDC-eseményeket és null tombstone-okat adott vissza |

Az automatikus integrációteszt parancsa:

```bash
python3 tests/verify_cdc.py
```

Az elkészítéskor végzett sikeres futás archivált jelentése: [docs/validation/validation.json](docs/validation/validation.json). A kiválasztott Kafka-üzenetek, a connector státuszai, a restart előtti/utáni offsetek, a scriptek ellenőrzési eredménye és a Kafka UI válasza a `docs/validation/` mappában a Git-repó része. A `validation.json` felsorolja ezeket a bizonyítékfájlokat. A Python-teszt 9901–9903 sorait siker után eltakarította; a recovery-demo a dokumentált 9002 sort RECOVERED állapotban hagyta.

Új tesztfuttatáskor az aktuális jelentés és a teljes napló az `artifacts/` mappába kerül. Ezeket a generált fájlokat a Git figyelmen kívül hagyja; a korábban átadott ZIP a teljes eredeti naplókat is tartalmazza.

## A tesztelés során javított részletek

- A MySQL init.sql a konténer felhasználója számára olvasható jogosultságot kapott. A hibás első inicializálásból származó, még üres lab-volume-ot újralétrehoztuk.
- A Debezium image által támogatott környezeti változónevekkel állítottuk be a schemaless JSON-convertereket és a Connect hirdetett címét.
- A Debezium 3.0.8 snapshotjelzői között `first`, `first_in_data_collection`, `last_in_data_collection` és `last` is szerepelhet. A teszt az image saját SnapshotRecord enumjához igazodik, és továbbra is megköveteli az `op=r` eseményt.
- Mindkét böngészős szolgáltatás kapott működő healthchecket. A Kafka UI ellenőrzésében a konténer BusyBox wgetje a helyi kérést proxy nélkül küldi.
- A Kafka console consumer szándékos idle timeoutja 1-es kilépési kódot és `TimeoutException` diagnosztikát ad. A scriptek csak ezt a felismert esetet fogadják el; a többi hibát továbbadják. Az `artifacts/*stderr.txt` ilyen elvárt timeoutja nem sikertelen CDC-teszt.

## Futtatási körülmények és korlátok

A felhő Docker-daemonja `vfs` drivert használ, amely a Debezium image sok rétegét teljes másolatokként tárolta, és a kezdeti pullnál betöltötte a rendelkezésre álló lemezt. A felhős teszthez az eredeti image egyetlen rétegbe összevont fájlrendszerét importáltuk az eredeti futtatási beállításokkal. Az eredeti linux/amd64 manifest és rétegeinek digestellenőrzése sikeres volt:

```text
quay.io/debezium/connect@sha256:0da513380a585aca6bb6561f0c18a225a61e7b53bc3d6d396ec8617c1ee95225
```

A Compose továbbra is a hivatalos `quay.io/debezium/connect:3.0.8.Final` image-et használja. A ZIP nem tartalmaz Docker-image-eket vagy adat-volume-okat: a laptop első indítása a hivatalos image-eket tölti le, és friss kezdeti adatokkal indul. A felhős workaround nem szükséges a szokásos Docker Desktop tárolódriver mellett.

A PowerShell-scriptek ténylegesen **PowerShell 7.4.6 alatt, Linuxon** futottak. A Windows PowerShell 5.1 támogatását kompatibilis szintaxissal és API-használattal készítettük el, de Windows gépen itt nem végeztünk futtatást. A böngészős szolgáltatásokat HTTP-n és API-n keresztül ellenőriztük, kézi grafikus böngészőtesztet nem állítunk. Más laptopon vagy új felhőtaskban történő indulást ez a jelentés nem igazol; a mellékelt scriptek és integrációteszt ott megismételhető.
