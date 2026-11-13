# Recovery

ISM fails closed: when it cannot prove a step worked, it stops changing that Immich server and says why.

| State (Servers page) | Meaning | What to do |
| --- | --- | --- |
| **Blocked** | Immich did not become quiet, or settings could not be changed. Nothing was modified. | Check Immich's jobs page, then **Retry**. |
| **Recovery required** | An item failed and undoing it also failed. Watchers stay off. | Open the operation, fix the listed files (each item says what it expected), click **Resolve** on the operation, then **Mark recovery as done** on the server. |
| **Partial** operation | Some items succeeded and some were rolled back. | Read the item errors; resubmit what is still wanted. |

After recovery is marked done, the normal quiet period, scan and settings restore follow.

If the container is restarted mid-operation, unfinished items are marked for recovery on start and the
server keeps watchers off until an admin confirms.

Settings ISM changed are stored before they are changed. If you must restore them by hand, re-enable
file watching and periodic scans in Immich's administration → External library settings.
