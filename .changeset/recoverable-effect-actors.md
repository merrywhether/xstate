---
"@xstate/effect": patch
---

Preserve accepted events, child replies, and queued action failures in Effect actor snapshots, including checkpoints before pending work has run. Resume captured work with the existing getter and restore APIs. Snapshot codecs preserve recovery metadata through existing schemas, including nested child state.

Restore pending Effect mailboxes with the Effect interpreter. Unsupported pending closures, captured values, or retired actor capabilities fail explicitly; accepted background action bodies are not persisted. Snapshots with pending recovery work require an updated reader.
