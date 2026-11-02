# VRConnect

Middleware on the MiniPC at the patient's bedside: reads vital signs from VitalRecorder and serves them over BLE to the mypredi tablet.

## Language

**MiniPC**:
The physical box wired to one patient monitor, running VRConnect (the GATE).
_Avoid_: GATE (for the box), PC, poste

**ecareTag**:
The identity of one physical **MiniPC**: its hostname as it was at first install, frozen from then on (a later rename or upgrade does not change it), and registered in ecare against a room. It follows the box if the box moves; a room change is an ecare-side edit only.
_Avoid_: advName, device name, room ID

## Relationships

- A **MiniPC** has exactly one **ecareTag**, for its whole life
- ecare maps an **ecareTag** to a room (and through the room, to a patient)
- A tablet must only reconnect to the **MiniPC** whose **ecareTag** it was assigned
- **Identity check**: before writing anything to a **MiniPC**, a tablet reads its **ecareTag** and walks away on mismatch

## Flagged ambiguities

- "advName" and "ecareTag" were proposed separately (user / Abdou) but name the same concept — resolved: **ecareTag**. "advName" now only means the raw BLE advertising name, which is one possible way to carry the ecareTag over the air.
