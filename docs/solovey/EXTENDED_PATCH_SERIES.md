# Extended patch series

All retained changes are ENGINE_PATCH_REQUIRED. Detailed problem, adapter boundary, regressions, exact base, origin and upstream/removal status live in each canonical commit message. Upstream contribution status is candidate/prepared, never submitted.

| Logical ID | Concern | Regression |
|---|---|---|
| EXTENDED-0001 | Preserve bounded SSM cache, failed-load state and cleanup | ./service/ssmapi |
| EXTENDED-0002 | Accept intentional SSM HTTP listener closure | ./service/ssmapi |
| EXTENDED-0003 | Preserve literal WebSocket paths and bound early data | ./transport/v2raywebsocket |
| EXTENDED-0004 | Preserve WireGuard IPC lifetime and AWG option projection | ./transport/wireguard; ./protocol/wireguard |
| EXTENDED-0005 | Drain Naive HTTP/3 parents before UDP closure | ./protocol/naive/quic; PANEL: ./service/coreinboundcontrol -run Naive |
| EXTENDED-0006 | Publish hot runtime objects only after serving-scope activation | ./adapter/... |
| EXTENDED-0007 | Accept intentional private API listener closure | ./service/api; ./adapter/... |
| EXTENDED-0008 | Honor Mieru serving ListenOptions and Scope cleanup | ./protocol/mieru |
| EXTENDED-0009 | Treat intentional TrustTunnel HTTP listener closure as normal | ./protocol/trusttunnel |
| EXTENDED-0010 | Keep Mieru inactive after its final user is revoked | ./protocol/mieru; PANEL: ./service -run Mieru |
| EXTENDED-0011 | Bind SSH sessions, pending handshakes and fallback to serving Scope | ./protocol/ssh |
| EXTENDED-0012 | Defer MTProxy workers until serving and retire peers with Scope | ./protocol/mtproxy -run TestMTProxy(Construction|Empty|Serving) |
| EXTENDED-0013 | Read the configured MTProxy polling interval independently of handshake timeout | ./protocol/mtproxy -run TestMTProxyDoppelGangerInterval |
| EXTENDED-0014 | Use a bounded HTTP timeout and honor the MTProxy dial override | ./protocol/mtproxy -run TestMTProxyHTTPHelper |
| EXTENDED-0015 | Retire Sudoku accepted transports, detached HTTP sessions/pull leases and reapers with serving Scope; retain inbound identity and TCP half-close | ./protocol/sudoku; ./transport/sudoku/... |
| EXTENDED-0016 | Give active Sudoku multiplex readers bounded drain grace while retaining the 4 MiB receive limit and stalled-stream reset | ./transport/sudoku/multiplex |
| EXTENDED-0017 | Validate resolved Sudoku protocol settings before acquiring an inbound, HTTP tunnel or listener | ./protocol/sudoku -run TestSudokuConstruction; PANEL: ./service -run TestSudokuCompleteCandidateRejectsBeforeOwnerCommit |

| EXTENDED-0018 | Validate Bond wire index/count and ratio bounds; reject overflowing outbound totals before constructing children | ./protocol/bond -run TestBond(Rejects\|Outbound) |
| EXTENDED-0019 | Preserve large/short Bond writes, bounded frames and caller read accounting | ./protocol/bond -run TestBond(Frames\|Read) |
| EXTENDED-0020 | Own Bond cache/admission, pending/routed legs and all-leg callbacks through serving Scope; reject duplicate/conflicting bundles | ./protocol/bond -run TestBond |

Inspect the current exact commits with git log --reverse d9442d14b19ddd02605bf3ee893a171947038e19..solovey/extended-patches. The queue verifier checks order, one-parent ancestry, pristine identity and the code tree.
