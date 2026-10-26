# Patch series range-diff and upstream update simulation

Status: PASS. Selected Shtorm extended remains d9442d14b19ddd02605bf3ee893a171947038e19. Fresh authority extended still points there; inspected extended-next/main/dev-next heads predate this selected release. No production base upgrade is made. The following candidates are explicitly synthetic newer descendants, never published or represented as actual upstream releases.

The documentation-only candidate c15d57f73464b2473b957fa71b2493fac7065d74 replays all fourteen source commits to 572b7a34ee4ff24f40d7cb792b0e0a67121283da. A full diff against 46c210b508ed08251bf70e7d78ae5e10427b7da8 contains only docs/solovey-update-simulation.txt. Every compiled source and regression-test byte is identical.

Command: git range-diff d9442d14b19ddd02605bf3ee893a171947038e19..46c210b508ed08251bf70e7d78ae5e10427b7da8 c15d57f73464b2473b957fa71b2493fac7065d74..572b7a34ee4ff24f40d7cb792b0e0a67121283da

```text
1:  b971aa5c =  1:  6371fd4b Preserve bounded SSM cache, failed-load state and cleanup
 2:  194dc691 =  2:  d32009bf Accept intentional SSM HTTP listener closure
 3:  7de74e52 =  3:  7d193563 Preserve literal WebSocket paths and bound early data
 4:  83005101 =  4:  7ea133c4 Preserve WireGuard IPC lifetime and AWG option projection
 5:  4c2cc46a =  5:  c3afe1fb Drain Naive HTTP/3 parents before UDP closure
 6:  9860d561 =  6:  1ce3835c Publish hot runtime objects only after serving-scope activation
 7:  d8c80155 =  7:  d0cd9b39 Accept intentional private API listener closure
 8:  e920bbbf =  8:  d73d9132 Honor Mieru serving ListenOptions and Scope cleanup
 9:  8db9a4c4 =  9:  6791388c Treat intentional TrustTunnel HTTP listener closure as normal
10:  ae2a167f = 10:  ca0799bc Keep Mieru inactive after its final user is revoked
11:  074eed2d = 11:  41baea7a Bind SSH sessions, pending handshakes and fallback to serving Scope
12:  74e15dac = 12:  3545f06e Defer MTProxy workers until serving and retire peers with Scope
13:  77d4c419 = 13:  fb8a11b5 Read the configured MTProxy polling interval independently of handshake timeout
14:  46c210b5 = 14:  572b7a34 Use a bounded HTTP timeout and honor the MTProxy dial override
```

Review disposition: fourteen UNCHANGED patches, no large drift. Core CI qualifies the identical source and test bytes; Panel acceptance still requires its own exact-copy consumption and runtime regressions.

A second synthetic candidate changes loadCache to return early. Its first patch replays cleanly at 4af22041d3fdc6f7a15c48e0fda265e5b5b43c3d, but the race-enabled TestCacheFailedStartPreservesPreimageAndCloseOnce exits 1: corrupt cache bypasses validation and reaches the absent test listener. This candidate is rejected. Textual clean application is insufficient evidence of correctness.

A third candidate f2a3dc829ecfb6e885d02917268e295e573c9bd8 changes the same upstream corrupt-cache cleanup statement removed by EXTENDED-0001. Cherry-pick exits 1, service/ssmapi/cache.go is unmerged, and the procedure stops at the first patch. The operation is aborted without accepting any patch or updating a Panel pin.

```text
Accepted patch commits after first conflict: 0. Git range-diff requires a nonempty second range; no accepted queue exists for this rejected candidate.
```

The selected production base and pristine ref stay unchanged. Future real updates must repeat range-diff review, classify every patch, run owned core CI and Panel provider/runtime regressions before pin acceptance. The simulation scratch refs and full negative logs are retained as migration evidence, not build authorities.

The simulations above qualify the original fourteen-patch ownership migration. Subsequent EXTENDED-0015, EXTENDED-0016 and EXTENDED-0017 are new Sudoku lifetime, bounded receive-flow and constructor-validation corrections on that accepted queue, not an upstream refresh; these historical simulation identities are not relabeled as proof of the later patches. Their own before/fixed regressions and repository CI qualify the new source deltas. A future upstream replay must review all seventeen patches for semantic obsolescence.

EXTENDED-0018 through EXTENDED-0020 add Bond admission, framing and Scope corrections after that historical simulation. Their own before/fixed probes and repository CI qualify them; the synthetic replay is not evidence for these later deltas. A future upstream replay must review all twenty source patches, including cleanly applying patches, for semantic obsolescence. The pristine base is unchanged.
