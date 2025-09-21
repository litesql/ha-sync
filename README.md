# ha-sync

SQLite extension to create **local read replicas** from [HA database](https://github.com/litesql/ha).

## Installation

Download **ha-sync** extension from the [releases page](https://github.com/litesql/ha-sync/releases).
Here's a great article that explains [how to install the SQLite extension.](https://antonz.org/install-sqlite-extension/)

### Compiling from source

- [Go 1.24+](https://go.dev) and CGO_ENABLED=1 is required.

```sh
go build -ldflags="-s -w" -buildmode=c-shared -o ha-sync.so
```

- Use .so extension for Linux, .dylib for MacOS and .dll for Windows

## Basic usage

### Loading the extension

```sh
sqlite3 mydatabase.db

# Load the extension
.load ./ha-sync

# check version (optional)
SELECT ha_info();
```

### Create a virtual table to start replication

```sql
CREATE VIRTUAL TABLE temp.ha USING HA(servers='nats://localhost:4222', timeout=5000, logger=stdout);

-- Insert the stream name into the created virtual table to start replication
INSERT INTO temp.ha(stream, durable) VALUES('ha_replication', 'my_instance');
```
