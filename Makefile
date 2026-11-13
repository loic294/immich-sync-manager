# Immich Sync Manager: development shortcuts (see AGENTS.md).

ISM_LOG ?= debug
ISM_DATA_DIR ?= .data
ISM_WEB_DIR ?= web/dist
ISM_MOUNTS_DIR ?= .data/mnt
ISM_LISTEN ?= 127.0.0.1:8080

.DEFAULT_GOAL := check

.PHONY: check check-rust check-web fmt gen-api dev-server dev-web image

check: check-rust check-web

check-rust:
	cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace

check-web:
	test -d web/node_modules || npm --prefix web ci
	npm --prefix web run check

fmt:
	cargo fmt --all && npm --prefix web run format

# Regenerates the TypeScript types from ism-core's DTOs (stale files are removed first).
gen-api:
	rm -f web/src/api/generated/*.ts
	cargo test -p ism-core export_bindings

dev-server:
	mkdir -p $(ISM_MOUNTS_DIR)
	ISM_LOG=$(ISM_LOG) ISM_DATA_DIR=$(ISM_DATA_DIR) ISM_WEB_DIR=$(ISM_WEB_DIR) \
	ISM_MOUNTS_DIR=$(ISM_MOUNTS_DIR) ISM_LISTEN=$(ISM_LISTEN) cargo run -p ism-server

dev-web:
	npm --prefix web run dev

image:
	docker build -f docker/Dockerfile -t immich-sync-manager:local .
