---
name: Bug report
about: Create a report to help us improve
title: ''
labels: bug
assignees: ''

---

**Environment**

- First line of `cdc-sink version` (specifically the git SHA):
- Source DB and version:
- Target DB and version:
- Staging DB and version, if separate from TargetDB:
- Load-balancer setup, if applicable:

**Describe the bug**
A clear and concise description of what the bug is.

**Expected behavior**
A clear and concise description of what you expected to happen versus what you observed.

**Diagnostic info**

Please connect to the cdc-sink diagnostic endpoint at `/_/diag` and include the JSON blob below.

```
{ ... DIAG DATA ... }
```

**User script**

If running a custom user-script, please add it here:

```
import * as api from "replicator@v1";
....
```

**Screenshots**
If applicable, add screenshots to help explain your problem.

**Additional context**
Add any other context about the problem here.
