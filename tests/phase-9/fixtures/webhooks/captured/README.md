# Captured Fixtures

Real Meta payloads land here during phase 10 (VPS / RunPod validation).
Each file is one verbatim delivery, with PII fields redacted to stable
hashes. Files are named `<sha256-of-body-prefix>.json`.

## How to record

A phase-10 utility will subscribe to the ingress tap and write the
body to this directory. Until then, this directory is empty and
`runtime-webhook-captured.test.mjs` is `it.skip`.

## How to redact

- `sender.id` → `sha256("user:" + id)[:16]`
- `recipient.id` → `sha256("page:" + id)[:16]`
- `from.username` → `sha256("uname:" + username)[:16]`
- `message.text` → keep first 32 chars, then `…`
- `comment.text` → same
- `media.media_url` → `https://redacted.invalid/<sha256[:16]>`
- `media_id` / `story_id` / `comment_id` → keep (these are the keys
  the runtime needs to correlate)

## How to commit

One file per delivery. Filename is the first 16 chars of the body's
sha256. Run the redactor over the file before committing.

## Why this matters

Captured fixtures are the highest-value fixtures in the entire test
suite. They encode Meta's actual behavior — including edge cases our
canonical fixtures do not anticipate. When this directory is
non-empty, the runtime is validated against the real world, not the
hand-crafted model.
