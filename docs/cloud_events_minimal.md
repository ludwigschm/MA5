# Minimalistische Cloud-Events

Die Cloud erhält nur die Felder `action`, `actor` und `player1_id`. Optional wird `session_id` ergänzt, wenn die Umgebung dies erzwingt. Alle anderen Eigenschaften werden zurückgewiesen und lokal protokolliert.

## Append-only Prinzip

* Ereignisse werden ausschließlich per `append_event` erstellt. Bestehende Einträge werden nicht verändert.
* Der `Idempotency-Key` muss für jeden Versuch identisch bleiben, damit Wiederholungen erkannt werden.
* Update-, Refine- oder Upsert-Aufrufe sind deaktiviert, solange `append_only_mode=True` aktiv ist.

## Troubleshooting „Redefines“

| Ursache | Check | Fix |
|---------|-------|-----|
| A | Payload enthält zusätzliche Felder | Nur `action`, `actor`, `player1_id` (+ `session_id`) senden |
| B | Client versucht `update_event` | Append-only Modus respektieren, statt Update neues Event senden |
| C | SDK/Backend akzeptiert Upserts | Sicherstellen, dass `upsert=false` gesetzt bleibt |
| D | Mehrfache Retries ohne Idempotency-Key | Für Retries denselben Key verwenden |
| E | Merge/Refine-API aufgerufen | APIs deaktivieren oder `append_only_mode` prüfen |
| F | Batching manipuliert bestehende Events | Batching deaktivieren oder Flags anpassen |
| G | Firmware liefert korrigierte Events | Firmware-Parameter prüfen und Append-only Fluss bestätigen |

## Checkliste für Projekt/Firmware/SDK Settings

1. `append_only_mode` aktiv?
2. Environment-Flags `SENDE_UPSERT`, `SENDE_MERGE`, `SENDE_BATCHING` deaktiviert?
3. Projekt-Flags `CLOUD_SESSION_ID_REQUIRED`, `EVENT_BATCH_SIZE`, `EVENT_BATCH_WINDOW_MS` prüfen.
4. Firmware-Versionen (z. B. `*_FIRMWARE_VERSION`) dokumentieren und auf unerwartete Defaults prüfen.
5. `Idempotency-Key`-Handling im Client testen (einzigartige Keys vs. Retries).
