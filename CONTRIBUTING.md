# Contributing to Walkrie

Thanks for considering a contribution. This document covers how to get a
development environment running, what's expected of a pull request, and
— importantly — the sign-off requirement below, which exists to keep
walkrie's licensing simple and unambiguous for everyone, including future
contributors.

## Developer Certificate of Origin (DCO)

**All commits must be signed off.** Walkrie does not require a separate
Contributor License Agreement (CLA) — instead, every commit must include
a `Signed-off-by` line certifying you wrote the contribution (or
otherwise have the right to submit it) under the terms below.

This matters for a concrete reason: walkrie is licensed under Apache 2.0
(see `LICENSE`), and the maintainer may in the future build separate,
proprietary features on top of walkrie's public interfaces, distributed
from a different, private repository under different license terms.
That only works cleanly if every contribution to *this* repository is
unambiguously and provably licensed under Apache 2.0 by its actual
author — a DCO sign-off is the standard, lightweight mechanism for that,
without the overhead of a formal CLA.

### What signing off means

By adding a `Signed-off-by` line to a commit, you certify the
[Developer Certificate of Origin, version 1.1](https://developercertificate.org/):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.

Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

### How to sign off

Add `-s` (or `--signoff`) to your commit command — git appends the line
automatically using your configured name and email:

```bash
git commit -s -m "Add batching support to XYZ"
```

This produces:
```
Add batching support to XYZ

Signed-off-by: Your Name <your.email@example.com>
```

If you forgot to sign off on a commit already made:
```bash
git commit --amend -s
```

For multiple commits in a branch:
```bash
git rebase --signoff HEAD~3   # adjust the number to how many commits need signing
```

Pull requests with unsigned commits will be asked to add sign-offs before
merge — this is a mechanical check, not a judgment on the contribution
itself.

## Getting started

See [TECHNICAL.md](./TECHNICAL.md) for full build instructions, including
the git submodule setup required for `llama.cpp`. In short:

```bash
git clone --recurse-submodules <repo-url> && cd walkrie
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j$(nproc)
```

## Running tests before submitting

* **Unit tests** (`walkrie_tests`, doctest — no live database required):
  ```bash
  ./walkrie_tests
  ```
* **Integration tests** (require a live Postgres instance and a real
  embedding provider — see `integration_tests/README.md`):
  ```bash
  ./test_sink_batch_mode <config.toml> --conninfo "<pg conninfo>"
  ```

A pull request that changes `PgEmbeddingSink`, `EventDispatcher`, or
anything touching batching/ordering behavior should include or update
relevant test coverage in both suites where applicable — these two
together are what caught the insert/delete-same-batch ordering bug
during batching development, and they're the primary regression guard
against reintroducing something similar.

## Pull request expectations

* Keep PRs focused — one logical change per PR is much easier to review
  than a bundle of unrelated fixes.
* If you're changing behavior (not just refactoring), explain *why* in
  the PR description, not just *what* changed.
* New config fields should be validated in `AppConfig::validate()` with a
  clear, actionable error message — see the existing `model_path` checks
  in `config.hpp` for the expected style (specific, tells the user what
  to do, not just "invalid config").
* If you're adding a new `EmbeddingProvider` or `SinkConfiguration`
  implementation, please follow the existing pattern (see
  `TECHNICAL.md`'s Sink Layer section) rather than introducing a new
  registration mechanism.

## Code style

No enforced formatter yet — match the surrounding code's style (brace
placement, naming conventions) within whatever file you're editing.
Prefer clarity over cleverness; this is infrastructure software people
will be debugging under production pressure.

## Reporting bugs / requesting features

Open a GitHub issue. For bugs, a minimal reproduction (config + steps)
is far more useful than a description alone — this project's own
development relied heavily on exact log output and reproducible
scenarios to track down subtle issues (see `PERFORMANCE.md` and
`integration_tests/README.md` for examples of the level of detail that
made past bugs tractable to fix).

## Questions

Open a GitHub discussion or issue — there's no separate chat/forum for
this project at this stage.
