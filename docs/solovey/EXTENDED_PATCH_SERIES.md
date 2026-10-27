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

| EXTENDED-0021 | Expose a snapshot of actual Scope-owned Bond members for effective readback and existing authenticated-parent controls | ./protocol/bond -run TestBondMemberSnapshotKeepsServingOwnership |

| EXTENDED-0022 | Interrupt blocked failover I/O and release rejected-leg UUID admission | ./protocol/failover -run TestFailover |
| EXTENDED-0023 | Own failover pending/routed legs and resumable session identity through serving Scope; snapshot actual children | ./protocol/failover -run TestFailoverScope |
| EXTENDED-0024 | Preserve complete-frame acknowledgments, large/short writes and bidirectional reconnect replay | ./protocol/failover -run TestFailoverFraming |
| EXTENDED-0025 | Bind outbound sessions, reconnect dialing and failed handshake transports to serving lifetime | ./protocol/failover -run TestFailover(NativeSession|CanceledHandshake) |
| EXTENDED-0026 | Notify rejected native failover leg owners exactly once across handler/router cleanup | ./protocol/failover -run TestFailoverRejectedLeg |

| EXTENDED-0027 | Keep connected failover sessions alive after completed dial cancellation; retain serving Scope retirement | ./protocol/failover -run TestFailover(NativeSession|CanceledHandshake) |

| EXTENDED-0028 | Own Call inbound worker, late bridge admission and pending platform HTTP/TLS transports through serving Scope | ./protocol/call -run TestCallInbound |

| EXTENDED-0029 | Preserve selected ResolveDialer query options through Call serving network ownership without inventing resolver support | ./protocol/call -run TestCall |

| EXTENDED-0030 | Own Call relay admission, pending streams and packet cleanup; preserve UDP datagram boundaries | ./transport/call/tunnel -run TestCall |
| EXTENDED-0031 | Retire native creator reconnect/signaling workers and use the selected dialer for VK WebTransport QUIC | ./protocol/call; ./transport/call/wtsignal -run TestCall |

| EXTENDED-0032 | Reject unusable Call platform/mode/credential candidates and unimplemented buffer constraints before dialer acquisition | ./protocol/call -run TestCallConstruction |
| EXTENDED-0033 | Keep room keys, signaling/auth responses and request URLs private; retain created room in trusted native state | ./transport/call/common -run TestCallDiagnostics; ./protocol/call -run TestCall |

| EXTENDED-0034 | Bind Call outbound setup/awaiters and late bridge to the serving Scope; synchronize publication and retire selected network transports | ./protocol/call -run TestCallOutbound |

| EXTENDED-0035 | Bind MTProxy background network callbacks to the serving context and drain/reject router admission at retirement without losing authenticated stream identity | ./protocol/mtproxy -run TestMTProxy |

Inspect the current exact commits with git log --reverse d9442d14b19ddd02605bf3ee893a171947038e19..solovey/extended-patches. The queue verifier checks order, one-parent ancestry, pristine identity and the code tree.
