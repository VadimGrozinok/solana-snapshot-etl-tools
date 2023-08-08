.PHONY: build start dev stop test mocks lint

SHELL := /bin/bash
BASEDIR = ${HOME}/solana/*.tar.zst

download:
	@rm ~/solana/*
	@cd solana-snapshot-finder/ && source ./venv/bin/activate && python3 snapshot-finder.py --snapshot_path ~/solana

stream:
	@for f in $(shell ls ${BASEDIR}); do cd solana-snapshot-etl/ && cargo r --features=standalone --bin=solana-snapshot-etl $${f} --geyser=./geyser-conf.json && date; done
