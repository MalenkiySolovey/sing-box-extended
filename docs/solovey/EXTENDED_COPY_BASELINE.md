# Extended copy baseline

SOURCE_AUTHORITY=https://github.com/shtorm-7/sing-box-extended

COPY_ROLE=mirror/cache/development

Copy: https://github.com/MalenkiySolovey/sing-box-extended. Upstream authorship, full Git ancestry, licenses and notices are retained. This is a source copy with a temporary patch queue, not an independent core product. The Panel owns selection, adapters, capabilities and exact build pins; this repository owns engine changes.

Selected upstream: v1.14.3-extended-2.7.3, d9442d14b19ddd02605bf3ee893a171947038e19, tree dae57a05c1392b57f03f03e987ed5807b45e14bc. Pristine ref: mirror/extended. Patch branch: solovey/extended-patches. It contains 14 ENGINE_PATCH_REQUIRED logical source commits and one repository policy/CI commit. The latter changes documentation/CI only. No upstream merge commits belong in this queue.

The pre-policy code tree 11e8da4309020a913e4ba69412631e6cf1465aa2 is byte-identical to every source and regression-test file in historical 541794dcd3e50f1254a09131bb47ddae01e2ffb7. Intentional removals are SOLOVEY_INTEGRATION.md and .github/workflows/solovey-integration.yml; their provenance and CI duties move here. No compiled behavior is removed.

All local code changes are upstream contribution candidates, prepared but not submitted. Removal requires equivalent upstream behavior plus the same passing regressions. External upstream PR submission needs operator authorization.

Pristine branches must receive only fast-forward synchronization to reviewed upstream points; patch/topic PRs target solovey/* branches. Branch protection should reject ordinary integration PRs into mirror/* while allowing the synchronization identity to fast-forward. No upstream or pristine ref is rewritten by this migration.
