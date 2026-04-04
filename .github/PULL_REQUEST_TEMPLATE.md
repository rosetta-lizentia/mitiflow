## Summary

<!-- What does this PR do? 1-3 sentences. -->

## Motivation

<!-- Why is this change needed? Link to issue if applicable. -->

## Changes

<!-- Bullet list of key changes. -->

-

## Review Checklist

### Author

- [ ] Self-reviewed the diff before requesting review
- [ ] Tests added or updated for new/changed behavior
- [ ] `cargo nextest run --workspace --features full` passes locally
- [ ] `cargo clippy --workspace --all-targets --features full -- -D warnings` is clean
- [ ] `cargo fmt --all -- --check` passes
- [ ] Documentation updated (doc comments, `docs/` if architectural)
- [ ] No `.unwrap()` / `.expect()` added in library code (tests/examples OK)
- [ ] Breaking changes noted below (if any)

### Reviewer

- [ ] Design: changes match the stated motivation
- [ ] Correctness: edge cases, error paths, and concurrency handled
- [ ] Tests: meaningful assertions, not just "doesn't panic"
- [ ] Naming: types, functions, and variables are clear and consistent with codebase
- [ ] Performance: no regressions on hot paths (publisher, subscriber, store)
- [ ] Zenoh patterns: key expressions follow conventions (`_` prefix for internal, `/p/` `/k/` structure)
- [ ] Feature gates: new store/backend code gated behind appropriate `#[cfg(feature = "...")]`

## Breaking Changes

<!-- If none, write "None." -->

None.

## Related Issues

<!-- Closes #N, Fixes #N, or Related to #N -->
