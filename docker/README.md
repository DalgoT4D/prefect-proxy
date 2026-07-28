# Prefect Job Runner Docker Image

Docker image for running Dalgo Prefect flows in EKS.

## What is this?

This image runs Dalgo Prefect flows on EKS. It installs all runtime deps
(prefect, prefect-dbt, prefect-shell, prefect-airbyte, dbt-core + adapters,
elementary-data, etc.) directly from `prefect-proxy/pyproject.toml` + `uv.lock`
— **single source of truth**. Bumping any dependency in pyproject flows into
the image on the next build with no Dockerfile edit.

`PrefectDbtRunner` uses `dbt-core` from the base env's Python — no per-version
dbt venvs baked into the image.

## How to Build

### Build Arguments

- `PREFECT_VERSION`: drives the base image tag (default: `3.6.29`). Must match
  the `prefect==` pin in `pyproject.toml` — otherwise pip will upgrade prefect
  on top of the base image.
- `CLIENTDBT_ROOT`: mount path for client dbt projects (default:
  `/mnt/appdata/clientdbts`).

### Image tags

Tags track the Prefect upgrade:

- `0.1` → Prefect 3.1.15
- `0.2` → Prefect 3.6.29

### Environment-Specific Builds

Dalgo's EKS runs on ARM nodes — build for `linux/arm64` for prod, `linux/amd64`
for local x86 testing.

**Build context is `prefect-proxy/`** (the parent of `docker/`) so
`pyproject.toml` and `uv.lock` are reachable.

```bash
# From the prefect-proxy directory
cd /path/to/prefect-proxy

# Prod build (EKS, ARM)
docker build --platform linux/arm64 -f docker/Dockerfile.job-runner \
  --build-arg PREFECT_VERSION=3.6.29 \
  -t tech4dev/prefect-eks-job-runner:0.2 .

# Local x86 test build
docker build --platform linux/amd64 -f docker/Dockerfile.job-runner \
  --build-arg PREFECT_VERSION=3.6.29 \
  -t tech4dev/prefect-eks-job-runner:0.2-amd64 .

# Multi-platform build + push (single tag, both arches)
docker buildx build --platform linux/arm64,linux/amd64 \
  -f docker/Dockerfile.job-runner \
  --build-arg PREFECT_VERSION=3.6.29 \
  -t tech4dev/prefect-eks-job-runner:0.2 \
  --push .

# Build with a custom shared-volume mount point
docker build --platform linux/arm64 -f docker/Dockerfile.job-runner \
  --build-arg PREFECT_VERSION=3.6.29 \
  --build-arg CLIENTDBT_ROOT=/dev/client/dbt \
  -t tech4dev/prefect-eks-job-runner:<tag> .
```

Sanity check the version pins inside a built image:
```bash
docker run --rm --platform linux/arm64 tech4dev/prefect-eks-job-runner:0.2 python -c "
import dbt.version, importlib.metadata as m
print('dbt-core:     ', dbt.version.__version__)
print('dbt-postgres: ', m.version('dbt-postgres'))
print('dbt-bigquery: ', m.version('dbt-bigquery'))
"
```
Values must match the pins in `prefect-proxy/pyproject.toml`. If they drift,
the lockfile export or install step isn't taking.

## Usage

Set this image in your EKS work pool configuration. Existing deployment
parameters work unchanged since the directory structure
(`/mnt/appdata/clientdbts/`) is preserved.
