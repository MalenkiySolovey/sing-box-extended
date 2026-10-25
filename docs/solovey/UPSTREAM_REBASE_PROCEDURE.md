# Upstream rebase procedure

1. Fetch upstream; select an exact authority tag/SHA and verify its tree. Fast-forward its pristine mirror ref only.
2. Preserve old base, patched commit and series identity under historical refs.
3. Create a candidate topic from the new exact base. Replay/rebase the small ordered commit queue. Stop on the first conflict; never auto-merge upstream into the queue.
4. Produce and review git range-diff OLD_BASE..OLD_PATCHED NEW_BASE..NEW_PATCHED. Classify each logical patch UNCHANGED, ADAPTED, DROPPED_UPSTREAMED, SPLIT or REMOVED_NOT_REQUIRED. Review semantics even when Git applies cleanly.
5. Re-run the same engine/race regressions, update the source-equivalence tree only after deliberate review, and run core CI.
6. Run Panel runtime/provider, Mieru/TrustTunnel, capability and release-provenance regression against the exact candidate. Only then change the Panel build pin.
7. Accept through a reviewed rebase/fast-forward of the controlled patch branch, retaining a linear queue. Do not create an upstream merge train. Do not rewrite the pristine ref.

Git commits are canonical. Generate git format-patch only for review, contribution or corresponding-source disclosure. Keep no second editable patch copy in Panel.
