# Events & Time Model

This document explains how gameplay UI events travel through the MA5 pipeline and how device
clock readings are preserved end to end.

## Design principles

* **Device time is the ground truth.** Every event is stamped with the device nanosecond clock
  as produced by the pupil bridge reconciliation layer. Host monotonic timestamps are treated as
  hints that allow us to derive the authoritative device time later on.
* **Host time is secondary.** We keep `t_ui_mono_ns` so that local diagnostics can relate UI
  updates to the originating interaction, but the downstream consumers (CSV, SQLite, cloud
  payloads) always prioritise the device timestamps computed by
  [`TabletopView._compute_device_time_fields`](../tabletop/tabletop_view.py).

## Event lifecycle

The standard path for a **critical** UI event, such as `card_flip`, is illustrated below:

1. **Capture host time:** Interaction handlers call
   [`TabletopView._build_event_payload`](../tabletop/tabletop_view.py) to snapshot the host
   monotonic time (`t_ui_mono_ns`) immediately before mutating the UI.
2. **Emit high-priority event:** The payload is enriched with device timing data by
   [`TabletopView._dispatch_ui_event`](../tabletop/tabletop_view.py), which resolves the
   [`UIEventSender`](../tabletop/logging/ui_events.py) and forwards it with
   `priority="high"`. Priority selection is governed by
   [`event_priority_for_action`](../tabletop/logging/policy.py), which treats the actions listed in
   `CRITICAL_ACTIONS` as low-latency sensitive when the environment permits it.
3. **Update UI:** UI mutations occur locally after the payload is queued, ensuring that the user
   interface reflects the interaction even if networking stalls.
4. **Persist locally:** `UIEventSender` writes the validated payload to both CSV and SQLite through
   [`UIEventLocalLogger.log`](../tabletop/logging/ui_events.py), preserving the derived device
   timestamps (`t_device_ns`, `t_device_vp1_ns`, `t_device_vp2_ns`).
5. **Forward to cloud:** The same payload is filtered by
   [`CloudClient.send_event`](../core/events/cloud_client.py) and dispatched immediately on the
   dedicated high-priority queue, guaranteeing that critical actions reach the backend without
   batching.

## Cloud payload

The cloud client only serialises the whitelisted keys defined in
[`core.events.cloud_client._ALLOWED_FIELDS`](../core/events/cloud_client.py). Any additional
metadata stays in the local CSV/SQLite stores.

### Allowed fields

* `session_id`
* `block_idx`
* `trial_idx`
* `actor`
* `player1_id`
* `action`
* `t_ui_mono_ns`
* `t_device_ns`
* `mapping_version`
* `mapping_confidence`

### Example payload

```json
{
  "session_id": "S123",
  "block_idx": 2,
  "trial_idx": 17,
  "actor": "VP1",
  "player1_id": "VP1",
  "action": "card_flip",
  "t_ui_mono_ns": 123456789012345,
  "t_device_ns": 123456781234567,
  "mapping_version": 8,
  "mapping_confidence": 0.94
}
```

## CSV schema

Local persistence uses [`UIEventLocalLogger`](../tabletop/logging/ui_events.py), which writes the
following columns via `SingleWriterLogger`:

| Column | Description |
| --- | --- |
| `session_id` | Session label used to partition events. |
| `block_idx` | Block counter within the session. |
| `trial_idx` | Trial counter within the block. |
| `actor` | Actor identifier (`P1`, `P2`, `Dealer`, …). |
| `player1_id` | Primary headset identifier associated with the actor. |
| `action` | Normalised action name (for example `card_flip`). |
| `t_ui_mono_ns` | Host monotonic timestamp captured before UI mutation. |
| `t_device_ns` | Selected authoritative device timestamp. |
| `t_device_vp1_ns` | Device timestamp resolved for headset `VP1`. |
| `t_device_vp2_ns` | Device timestamp resolved for headset `VP2`. |
| `mapping_version` | Mapping revision used by the reconciler. |
| `mapping_confidence` | Confidence score returned by the reconciler. |
| `mapping_rms_ns` | Root mean square error of the mapping in nanoseconds. |
| `t_utc_iso` | UTC wall-clock time of enrichment (ISO 8601). |
| `sequence_no` | (Local only) Monotonic counter maintained per `(session_id, actor)` pair. |

### Sample record

```csv
session_id,block_idx,trial_idx,actor,player1_id,action,t_ui_mono_ns,t_device_ns,t_device_vp1_ns,t_device_vp2_ns,mapping_version,mapping_confidence,mapping_rms_ns,t_utc_iso,sequence_no
S123,2,17,VP1,VP1,card_flip,123456789012345,123456781234567,123456781234567,,8,0.94,3500,2024-03-14T09:26:53.482Z,12
```

## Configuration matrix

| Setting | Source | Default | Effect | Notes |
| --- | --- | --- | --- | --- |
| `LOW_LATENCY_DISABLED` / `LOW_LATENCY_OFF` | Env vars checked by [`is_low_latency_disabled`](../tabletop/utils/runtime.py) | `0` | When set to `1`, disables the critical-event fast path; all events are treated as normal priority. | Use only for debugging; breaks low-latency guarantees. |
| `EVENT_BATCH_WINDOW_MS` | [`core.config.EVENT_BATCH_WINDOW_MS`](../core/config.py) | `0` (immediate) | Controls the flush window for normal-priority batches in [`CloudClient`](../core/events/cloud_client.py). | Critical actions ignore batching via the high-priority queue. |
| `EVENT_BATCH_SIZE` | [`core.config.EVENT_BATCH_SIZE`](../core/config.py) | `20` | Maximum number of normal-priority events sent per batch. | Ignored for `priority="high"` events. |
| `QC_RMS_NS_THRESHOLD` | [`core.config.QC_RMS_NS_THRESHOLD`](../core/config.py) | `5000` | Reference threshold for acceptable mapping error (nanoseconds). | Drives diagnostics and alerting. |
| `QC_CONFIDENCE_MIN` | [`core.config.QC_CONFIDENCE_MIN`](../core/config.py) | `0.9` | Minimum acceptable confidence for reconciled mappings. | Used when evaluating mapping quality metrics. |

### Critical event policy

Critical events are defined by `CRITICAL_ACTIONS` inside
[`tabletop.logging.policy`](../tabletop/logging/policy.py). When low latency is enabled, those
actions are sent with `priority="high"` and bypass normal batching (`CloudClient` drains the
high-priority queue immediately). This ensures that the device time recorded in CSV, SQLite, and
cloud payloads reflects the true device ordering of gameplay interactions.
