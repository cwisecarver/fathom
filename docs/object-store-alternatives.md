# Replacing MinIO — investigation (TABLED 2026-09-30)

**Status: tabled.** The current setup works, so nothing changes yet. This records what the
investigation found so the decision does not have to be re-researched.

## Why this came up

Upstream MinIO went into maintenance mode in December 2025, was marked "no longer maintained" on
2026-02-12, and its community images are gone: `minio/minio` on Docker Hub no longer exists and
`quay.io/minio/minio` answers 401. Fathom's `:s3` suite, the chaos rig and
`scripts/benchmark_s3_latency.sh` run against Chainguard's source-built MinIO
(`cgr.dev/chainguard/minio`, mirrored to `ghcr.io/cwisecarver/minio`, see `aa440d1`). Production
talks to real S3; the local store only has to be **faithful to S3** for tests and the rig.

## The requirement that decides it

- **The same store locally and in CI.** One image, so a result on a laptop and a result on a runner
  mean the same thing.
- **Conditional writes, enforced.** The single-writer design (`Fathom.Shard.Storage`,
  [single-writer.md](single-writer.md)) rests on `PUT If-None-Match: *` (create a lock or object only
  if absent), `PUT If-Match: <etag>` (the flush and lock fences), conditional `DELETE` (lease
  release) and conditional `CopyObject` (the steal-time touch, restores). A store that **accepts**
  these headers but does not **enforce** them is worse than one that rejects them: the suite goes
  green while every fence is a no-op.
- Also used: `HEAD` returning `ETag` and `x-amz-meta-*` (the position and lineage stamps), a `Date`
  response header (the skew-safe steal clock), `ListObjectsV2`.

## Candidates

2026 community sentiment is from Hacker News, Reddit (r/selfhosted, r/homelab) and 2026 roundups;
sources are listed at the end. "Unverified" means neither docs nor issues settled it.

| Candidate | 2026 sentiment | License / maintenance | Conditional writes | Verdict for fathom |
|---|---|---|---|---|
| **pgsty/silo** (was pgsty/minio, renamed 2026-08-06) | The community-fork answer; Dokploy moved its templates to it | AGPL-3.0; one maintainer; releases every 1–2 months (latest 20260903, April 2026 CVE fixes); multi-arch `pgsty/silo` on Docker Hub | Unverified in its docs, but it is MinIO's code, and MinIO added conditional writes in 2024 | **Lowest-risk swap** — the behaviour the suite already passed against |
| **SeaweedFS** | The most-recommended "mature" pick; `weed mini` suggested for dev/CI; some see it as one-person-driven | Apache-2.0, Go, since 2012, active | **Yes, per the vendor** (2026-07-16): PUT, COPY and DELETE `If-Match`/`If-None-Match`. **Needs ≥ 4.09**: older versions do not enforce `If-Match`, and a January 2026 bug skipped the check with versioning on (fixed) | **Best forward option**, pending our own probe; `x-amz-meta-*` on HEAD unverified |
| **RustFS** | "Closest MinIO replacement", the rising favourite; people distrust its CLA and marketing-heavy docs | Apache-2.0, Rust, young | **Partial.** 1.0.0 enforces PUT `If-None-Match: *` and `If-Match`; it rejected unquoted ETags (Jan 2026); a beta failed under lock contention; conditional DELETE/COPY unconfirmed | Only after it passes the probe |
| **Garage** | Loved for small self-hosted clusters | AGPLv3, Rust, active | **No.** A mismatched `If-Match` returns 200, not 412; the maintainers doubt it can be done safely (issue 1052) | **Ruled out** — our fences would silently stop working |
| Ceph RGW | "Bulletproof but too heavy" | LGPL, very active | Unverified | Not a single small container |
| Apache Ozone | Little discussion | Apache-2.0 | Designed (Jan 2026), not shown shipped | JVM-heavy; not for dev |
| Versity S3 Gateway, Zenko CloudServer, s3proxy | No 2026 discussion found | — | Unverified | Gateways inherit their backend's semantics |
| LocalStack S3 | — | — | No evidence | An emulator, not a store |

## Where it landed

- The community shortlist is SeaweedFS, RustFS and Garage. The roundups mostly do not check
  conditional writes, which is exactly where these differ for us.
- **When this is picked back up:** `pgsty/silo` is the drop-in (same code as today); SeaweedFS ≥ 4.09
  is the forward-looking choice (Apache-licensed, mature). Use **the same image locally and in CI**.
- **Whatever is chosen, gate on a probe first.** A startup check run against the exact image tag:
  - a second `PUT If-None-Match: *` returns 412;
  - a `PUT` with a mismatched `If-Match` returns 412;
  - `DELETE` and `CopyObject` with a mismatched `If-Match` are refused;
  - `HEAD` returns the `ETag`, the `x-amz-meta-*` we wrote, and a `Date` header.

  If the probe fails, the store is wrong for fathom no matter how the suite looks.

## Sources (dated)

- HN discussion, ~Feb 2026 — <https://news.ycombinator.com/item?id=47000041>
- pgsty on the MinIO fork, 2026-02 → 2026-09 — <https://blog.vonng.com/en/db/minio-resurrect/>,
  <https://vonng.com/en/db/minio-promise-kept/>, <https://github.com/pgsty/silo>
- SeaweedFS conditional writes, 2026-07-16 — <https://www.seaweedfs.com/blog/conditional-writes/>
- SeaweedFS #8073 (versioning skipped the check), 2026-01-21 — <https://github.com/seaweedfs/seaweedfs/issues/8073>
- RustFS #1458 (unquoted ETags), 2026-01-09 — <https://github.com/rustfs/rustfs/issues/1458>
- NotedThat #190 (cross-store conditional-write findings), 2026-09-24 — <https://github.com/NotedThat/NotedThat/issues/190>
- Apache Ozone PR 9334 (design), 2026-01-15 — <https://github.com/apache/ozone/pull/9334>
- barrel PR 64 (MinIO image gone; RustFS in CI), 2026 — <https://github.com/barrel-db/barrel/pull/64>
- Garage issues 1052 and 804 (undated in results) — <https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/1052>,
  <https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/804>
- 2026 roundups (dates not shown; low confidence): u11d, elest.io, Akmatori, Rilavek
