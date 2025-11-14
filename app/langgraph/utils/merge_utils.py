from __future__ import annotations
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

# ------------------------------
# Profile merge
# ------------------------------
def merge_profile(
    ephemeral: Dict[str, Any],
    db_profile: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    merged = dict(db_profile or {})
    changes = 0

    for k, v in (ephemeral or {}).items():
        if v in (None, "", [], {}):
            continue

        conf = 1.0
        if isinstance(v, dict) and "value" in v and "confidence" in v:
            conf = float(v.get("confidence", 1.0))
            v = v.get("value")

        if conf < 0.7:
            continue

        if merged.get(k) != v:
            merged[k] = v
            changes += 1

    merged["_merge_changes"] = changes
    merged["updated_at"] = datetime.now(timezone.utc)
    return merged


# ------------------------------
# Collection(triple) merge
# ------------------------------
def merge_collection(
    ephemeral: Any,
    db_coll: Optional[List[Dict[str, Any]]]
) -> Dict[str, Any]:

    existing_triples = list(db_coll or [])
    merged = list(existing_triples)

    existing_keys = set()
    for t in existing_triples:
        if not isinstance(t, dict):
            continue
        key = (
            (t.get("subject") or "").strip(),
            (t.get("predicate") or "").strip(),
            (t.get("object") or "").strip(),
            (t.get("code_system") or "") or "",
            (t.get("code") or "") or "",
        )
        existing_keys.add(key)

    # ephemeral 형태 정리
    if isinstance(ephemeral, dict) and "triples" in ephemeral:
        new_triples = ephemeral["triples"]
    elif isinstance(ephemeral, list):
        new_triples = ephemeral
    else:
        new_triples = []

    changes = 0

    for t in new_triples:
        if not isinstance(t, dict):
            continue
        subj = (t.get("subject") or "").strip()
        pred = (t.get("predicate") or "").strip()
        obj  = (t.get("object") or "").strip()
        cs   = (t.get("code_system") or "") or None
        cd   = (t.get("code") or "") or None

        if not subj or not pred or not obj:
            continue

        key = (subj, pred, obj, cs or "", cd or "")
        if key in existing_keys:
            continue

        existing_keys.add(key)
        merged.append({
            "subject": subj,
            "predicate": pred,
            "object": obj,
            "code_system": cs,
            "code": cd,
        })
        changes += 1

    return {
        "triples": merged,
        "_merge_changes": changes,
    }