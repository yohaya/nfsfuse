# nfsfuse

A lightweight, single-binary NFS client that mounts NFS exports as local filesystems using FUSE3 and [libnfs](https://github.com/sahlberg/libnfs). No kernel NFS support or root privileges required (beyond FUSE access).

## Features

- **Single static binary** — no dependencies, runs on Debian 8+ and any modern Linux
- **NFSv3 and NFSv4** support
- **Userspace NFS** — uses libnfs instead of the kernel NFS client
- **Performance mode** (`--max`) — per-file NFS contexts, kernel caching, writeback, large I/O chunks
- **NFSv4 safe mode** — automatically forces single-threaded operation and disables caching for NFSv4

## Download

Pre-built static binaries are available in the [`bin/`](bin/) directory or from [GitHub Releases](https://github.com/yohaya/nfsfuse/releases).

```bash
git clone https://github.com/yohaya/nfsfuse.git
chmod +x nfsfuse/bin/nfsfuse
```

## Usage

```
nfsfuse [--max] [--debug] nfs://server/export/path[?version=3|4] <mountpoint> [FUSE options]
```

### Options

| Option | Description |
|-----------|----------------------------------------------|
| `--max` | Enable performance optimizations |
| `--debug` | Print version info and debug tracing to stderr |
| `--version`| Show version, libnfs version, and build info |
| `-f` | Run in foreground (FUSE option) |
| `-d` | Enable FUSE debug output (FUSE option) |
| `-s` | Force single-threaded mode (FUSE option) |

### Examples

Mount an NFSv3 export:
```bash
nfsfuse 'nfs://192.168.1.100/export/data?version=3' /mnt/nfs
```

Mount an NFSv4 export:
```bash
nfsfuse 'nfs://192.168.1.100/export/data?version=4' /mnt/nfs
```

Mount with performance optimizations:
```bash
nfsfuse --max 'nfs://192.168.1.100/export/data?version=3' /mnt/nfs
```

Debug a connection:
```bash
nfsfuse --debug 'nfs://192.168.1.100/export/data?version=4' /mnt/nfs
```

Run in foreground with FUSE debug output:
```bash
nfsfuse 'nfs://192.168.1.100/export/data' /mnt/nfs -f -d
```

Unmount:
```bash
fusermount -u /mnt/nfs
```

## Performance mode (`--max`)

When `--max` is enabled (NFSv3 or NFSv4):

- Each open file/directory gets its own NFS connection for full parallelism
- Kernel page cache and writeback cache are enabled
- Attribute and entry caching with 30-second timeouts
- I/O chunk size auto-tuned up to 1 MB based on server limits
- Directory readahead buffers enabled (NFSv3)

Without `--max`, a single shared NFS connection is used with conservative caching.

## NFSv4 notes

- NFSv4 automatically forces single-threaded FUSE operation (`-s`) because libnfs NFS contexts are not thread-safe
- Directory caching is disabled for NFSv4 to avoid stale data
- When using `--max` with NFSv4, per-file connections are used but kernel caching remains disabled

## Requirements

The target system needs:
- Linux kernel with FUSE support (`/dev/fuse`)
- `fusermount3` or `fusermount` installed (usually from `fuse3` or `fuse` package)

```bash
# Debian/Ubuntu
apt-get install fuse3

# CentOS/RHEL
yum install fuse3
```

## Building from source

The binary is built automatically by GitHub Actions. To build locally, you need `libfuse3-dev` and `libnfs-dev`:

```bash
# Install build dependencies (Debian/Ubuntu)
apt-get install gcc libfuse3-dev libnfs-dev pkg-config

# Compile
gcc -O2 -Wall -o nfsfuse nfsfuse.c \
    $(pkg-config --cflags --libs fuse3 libnfs) \
    -lpthread

# Or with version info
gcc -O2 -Wall -o nfsfuse nfsfuse.c \
    -DNFSFUSE_VERSION='"1.0.0"' \
    -DNFSFUSE_BUILD='"1"' \
    -DNFSFUSE_LIBNFS_VERSION='"system"' \
    -DNFSFUSE_BUILD_DATE='"$(date -u)"' \
    -DNFSFUSE_BUILD_HOST='"$(hostname)"' \
    $(pkg-config --cflags --libs fuse3 libnfs) \
    -lpthread
```

## Supported operations

| Operation | Description |
|-----------|--------------------------|
| getattr | Stat files and directories |
| opendir / readdir / releasedir | List directory contents |
| open / create / release | Open, create, close files |
| read / write | Read and write file data |
| truncate | Truncate files |
| unlink | Delete files |
| mkdir / rmdir | Create and remove directories |
| rename | Rename files and directories |
| statfs | Filesystem statistics |

## License

See source for details.
