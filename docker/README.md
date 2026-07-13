# Prefect Job Runner - Multi-dbt Docker Image

Docker image for running Dalgo Prefect flows in EKS with support for multiple dbt versions.

## What is this?

This image runs Dalgo Prefect flows on EKS. `dbt-core` + the postgres/bigquery adapters are installed directly in the worker's Python env — `PrefectDbtRunner` uses them from Python, no separate per-version venvs are baked into the image. All orgs converge on the single dbt-core version pinned in the Dockerfile.

## How to Build

### Build Arguments

`prefect` itself is provided by the base image (`prefecthq/prefect:<PREFECT_VERSION>-python3.10-kubernetes`); the integration packages are installed directly on top via the args below — there is no longer a `prefect-integrations/` bundle to maintain. To cut a new image, bump the version args together.

- `PREFECT_VERSION`: Prefect version — drives the base image (default: 3.6.29)
- `PREFECT_DBT_VERSION`: prefect-dbt version (default: 0.7.24). `dbt-core`, `dbt-postgres`, and `dbt-bigquery` are pinned explicitly in the Dockerfile rather than via `prefect-dbt` extras — bump the pin lines directly when upgrading dbt.
- `PREFECT_SHELL_VERSION`: prefect-shell version (default: 0.3.5)
- `PREFECT_AIRBYTE_REF`: git ref (tag/branch) of the `Ishankoradia/prefect-airbyte` fork (default: v0.90)
- `CLIENTDBT_ROOT`: Path for client dbt projects (default: /mnt/appdata/clientdbts)

### Image tags

Tags track the Prefect upgrade:

- `0.1` → Prefect 3.1.15
- `0.2` → Prefect 3.6.29

### Environment-Specific Builds

Dalgo's EKS runs on ARM nodes — build for `linux/arm64` for prod, `linux/amd64` for local x86 testing.

```bash
# From the prefect-proxy/docker directory
cd /path/to/prefect-proxy/docker

# Prod build (EKS, ARM). Integration versions default per the args above.
docker build --platform linux/arm64 -f Dockerfile.job-runner \
  --build-arg PREFECT_VERSION=3.6.29 \
  -t tech4dev/prefect-eks-job-runner:0.2 .

# Local x86 test build
docker build --platform linux/amd64 -f Dockerfile.job-runner \
  --build-arg PREFECT_VERSION=3.6.29 \
  -t tech4dev/prefect-eks-job-runner:0.2-amd64 .

# Multi-platform build + push (single tag, both arches)
docker buildx build --platform linux/arm64,linux/amd64 \
  -f Dockerfile.job-runner \
  --build-arg PREFECT_VERSION=3.6.29 \
  -t tech4dev/prefect-eks-job-runner:0.2 \
  --push .

# Override integration versions explicitly
docker build --platform linux/arm64 -f Dockerfile.job-runner \
  --build-arg PREFECT_VERSION=3.6.29 \
  --build-arg PREFECT_DBT_VERSION=0.7.24 \
  --build-arg PREFECT_SHELL_VERSION=0.3.5 \
  --build-arg PREFECT_AIRBYTE_REF=v0.90 \
  -t tech4dev/prefect-eks-job-runner:0.2 .

# Build with a custom shared-volume mount point
docker build --platform linux/arm64 -f Dockerfile.job-runner \
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
Expect `1.10.19 / 1.10.2 / 1.10.3`. If any drift, the pin isn't taking.

## Usage

Set this image in your EKS work pool configuration. Existing deployment parameters will work unchanged since the directory structure (`/home/ddp/dbt/`, `/mnt/appdata/clientdbts/`) is preserved.