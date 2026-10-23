<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/brand/wordmark-dark.svg">
    <img src="assets/brand/wordmark.svg" alt="Duct" width="320">
  </picture>
</p>

# Duct

Open-source, local-first data platform project by Tucunar Labs. CLI: `duct`.
Built on DuckDB and DuckLake, written in Go, with a Python SDK for user code.

**Status: design stage.** Duct is still a skeleton: it has contracts, a registry, one
example module and the CLI shell; there is no engine yet.

Design decisions live in [docs/adr](docs/adr/README.md); repo layout in
[ADR 0015](docs/adr/0015-estructura-del-repo.md).

## Build and check

Tools are pinned in `mise.toml` (`mise install`).

```sh
just check   # generate, fmt, vet, lint, short tests
just build   # bin/duct
./bin/duct modules
```

## License

Apache-2.0.