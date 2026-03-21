# CLAUDE.md — nfsfuse project instructions

## Repository

- GitHub: https://github.com/yohaya/nfsfuse
- Single-file C project: `nfsfuse.c`
- FUSE3 filesystem bridging FUSE to NFS via libnfs
- Builds as a fully static Linux binary via GitHub Actions

## Build & Deploy Workflow

**After every code change to nfsfuse.c (or any source file), you MUST:**

1. Commit the changes to a branch (feature branch or main)
2. Push to GitHub
3. Wait for the GitHub Actions build workflow to complete (`gh run watch`)
4. Verify the build passes (check for compile errors and warnings)
5. If the build fails, fix the errors and push again

The workflow (`.github/workflows/build.yml`) does:
- Auto-bumps the patch version in `VERSION` on push to `main`
- Builds a static binary in Alpine Linux (gcc, libnfs, libfuse3)
- Commits the binary (`bin/nfsfuse`) and updated `VERSION` to the repo on `main`

**For non-trivial changes:** Create a feature branch, push, verify the PR build passes, then merge to main.

**For small fixes:** Can push directly to main if confident.

## Build commands (local reference)

The project does not build locally on macOS — it requires Linux with libnfs and libfuse3. All builds go through GitHub Actions. Use:

```sh
gh run list --limit 5          # check recent builds
gh run watch <run-id>          # watch a build in progress
gh run view <run-id> --log     # check build logs for warnings/errors
```

## Code conventions

- C99, compiled with `-O2 -Wall`
- Single source file: `nfsfuse.c`
- No external test suite — testing is done by deploying to production nodes
- Warnings should be zero (except `-Wunused-function` for utility functions kept for future use)
