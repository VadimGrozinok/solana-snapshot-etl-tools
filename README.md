# Solana snapshot ETL tools
This repository collects 3 necessary things to fill any DB with current state of all the Solana accounts.

[Solana-snapshot-etl](https://github.com/terorie/solana-snapshot-etl) and [solana-snapshot-finder](https://github.com/c29r3/solana-snapshot-finder) were NOT written by me. Shout-out to guys and thanks for their work.

```
sudo apt-get install -y build-essential libsasl2-dev pkg-config libssl-dev \
&& git submodule update
```

### Download snapshot
```bash
sudo apt-get update \
&& sudo apt-get install python3-venv -y \
&& python3 -m venv venv \
&& source ./venv/bin/activate \
&& pip3 install -r requirements.txt
```

```bash
python3 snapshot-finder.py --snapshot_path $HOME/solana/validator-ledger
```

### Compile Geyser plugin
```bash
cargo b --release
```

### Start solana snapshot etl
```bash
cargo r --features=standalone --bin=solana-snapshot-etl snapshot-139240745-*.tar.zst --geyser geyser-conf.json
```