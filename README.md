# Airbyte Source: Sage Intacct

Standalone Airbyte Source connector for Sage Intacct XML Web Services.
Requires Python 3.12.

## Features

- Streams:
  - `gl_journals`
  - `gl_batches`
  - `gl_entries`
  - `gl_detail`
  - `customers`
  - `ar_invoices`
  - `ar_invoice_items`
  - `oe_invoices`
  - `oe_invoice_lines`
  - `orders`
  - `order_lines`
  - `subtotals`
  - `so_subtotals`
- Multi-entity sync through `readEntityDetails`
- Incremental sync using `WHENMODIFIED`
- 3-day default lookback (configurable)

## Config

Required:

- `sender_id`
- `sender_password`
- `company_id`
- `user_id`
- `user_password`

Optional:

- `api_url` (default: `https://api.intacct.com/ia/xml/xmlgw.phtml`)
- `start_date` (RFC3339; default now-30d)
- `lookback_days` (default: 3)
- `page_size` (default: 1000, max 1000)
- `slice_step_days` (default: 7)
- `entities_mode` (`all` or `selected`)
- `entity_ids` (list, used with `selected`)
- `oe_invoice_docparid` (default: `Sales Invoice`)
- `oe_order_docparid` (default: `Sales Order`)

## Local commands

```bash
docker buildx build --platform linux/amd64 -t source-sage-intacct:dev --load .
docker run --rm -i source-sage-intacct:dev spec
docker run --rm -i source-sage-intacct:dev check --config /data/config.json
docker run --rm -i source-sage-intacct:dev discover --config /data/config.json
docker run --rm -i source-sage-intacct:dev read --config /data/config.json --catalog /data/catalog.json --state /data/state.json
```

## Development

### uv

```bash
uv sync --dev
uv run ruff check .
uv run pytest -q
```

### Nix dev shell

```bash
nix develop
uv sync --dev
```

## Integrate with Airbyte

Airbyte runs connectors as containers for `check`, `discover`, and `read` jobs.
You do not run a long-lived connector container manually, but you do need a connector image available to your Airbyte runtime.

### Local Docker Compose Airbyte

1. Build the image:
   ```bash
   docker buildx build --platform linux/amd64 -t airbyte/source-sage-intacct:dev --load .
   ```
2. In Airbyte UI, add a custom source connector and set:
   - Repository: `airbyte/source-sage-intacct`
   - Tag: `dev`

### Kubernetes + Helm Airbyte

For Helm deployments, Airbyte worker/job pods must be able to pull your connector image from a registry.

1. Build and push your image to a registry reachable by the cluster (internal registry is supported):
   ```bash
   docker buildx build --platform linux/amd64 -t registry.mdomain.com/airbyte/source-sage-intacct:0.1.1 --push .
   ```

2. Create pull-secret(s) for that registry:
   ```bash
   kubectl create secret docker-registry regcred \
     --namespace <airbyte-namespace> \
     --docker-server=registry.mdomain.com \
     --docker-username=<username> \
     --docker-password=<password> \
     --docker-email=<email>
   ```
   If your Helm values use the default workload namespace (`global.workloads.namespace: jobs`), create the same secret in `jobs` too:
   ```bash
   kubectl create secret docker-registry regcred \
     --namespace jobs \
     --docker-server=registry.mdomain.com \
     --docker-username=<username> \
     --docker-password=<password> \
     --docker-email=<email>
   ```

3. Configure Helm so Airbyte can use this pull secret:
   ```yaml
   global:
     imagePullSecrets:
       - name: regcred
   worker:
     kube:
       mainContainerImagePullSecret: regcred
   ```
   Then apply with your normal Helm upgrade/install flow.

4. In Airbyte UI, add a custom source connector:
   - Repository: `registry.mdomain.com/airbyte/source-sage-intacct`
   - Tag: `0.1.1`

5. Create a source using this connector and run `Check connection`.

## CI/CD

- PR lint: `.github/workflows/lint-pr.yml` uses `uv` and runs `ruff`.
- Release automation: `.github/workflows/release-please.yml` uses Conventional Commits to open release PRs and create semver tags/releases.
- Docker publish on tags: `.github/workflows/docker-publish.yml` builds and pushes multi-arch images to `ghcr.io/<owner>/source-sage-intacct` for tags matching `v*.*.*`.
