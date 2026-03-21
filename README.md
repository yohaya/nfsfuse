# nfsfuse

A lightweight, single-binary NFS client that mounts NFS exports as local filesystems using FUSE3 and [libnfs](https://github.com/sahlberg/libnfs). No kernel NFS support or root privileges required (beyond FUSE access).

## Features

- **Single static binary** — no dependencies, runs on Debian 8+ and any modern Linux
- **NFSv3 and NFSv4** support via libnfs (userspace NFS client)
- **Auto-reconnect** — recovers from ESTALE, EIO, NFS4ERR_EXPIRED, NFS4ERR_GRACE, and NFS4ERR_BADHANDLE automatically (enabled by default)
- **Dead-server watchdog** (`--dead-timeout`) — detects stuck NFS calls, shuts down the socket, and returns EIO to unblock applications
- **NFS4 lease keepalive** — background thread renews NFSv4 leases every 30 seconds to prevent session expiry during long I/O
- **Deferred close pool** — keeps recently closed file handles alive for NFSv4 open-close consistency
- **Performance mode** (`--max`) — larger I/O chunks, kernel page caching (NFSv3), background requests
- **Syslog logging** (`--log-errors`) — structured error/retry/recovery logging with mountpoint identification
- **Configurable retry policy** — tune retry count and wait time for NFS4 transient errors
- **Debug tracing** — three verbosity levels, output to stderr, file, or syslog

## Download

Pre-built static binaries are available in the [`bin/`](bin/) directory or from [GitHub Releases](https://github.com/yohaya/nfsfuse/releases).

```bash
git clone https://github.com/yohaya/nfsfuse.git
chmod +x nfsfuse/bin/nfsfuse
```

## Quick start

```bash
# Simple NFSv4 mount
nfsfuse 'nfs://192.168.1.100/export/data?version=4' /mnt/nfs

# Production mount with resilience
nfsfuse --dead-timeout 120 --timeout 15000 --retrans 5 \
  --autoreconnect 100 --log-errors \
  --noatime --nodiratime --noexec \
  'nfs://192.168.1.100/export/data?version=4' /mnt/nfs

# Unmount
fusermount3 -u /mnt/nfs
```

## Usage

```
nfsfuse [options] nfs://server/export/path[?version=3|4] <mountpoint> [FUSE options]
```

### General options

| Option | Description |
|--------|------------|
| `--max` | Enable performance optimizations (larger I/O, caching for NFSv3) |
| `--debug [level]` | Debug tracing (1=config/reconnect, 2=all operations, 3=detailed params) |
| `--debug-output <dst>` | Send debug to file path or `syslog` instead of stderr |
| `--log-errors` | Log NFS errors and retry lifecycle to syslog (daemon facility) |
| `--noatime` | Do not update access time on read |
| `--nodiratime` | Do not update directory access time |
| `--noexec` | Disallow execution of binaries on mount |
| `--writeback-cache` | Enable kernel writeback cache (faster writes, risk of data loss on crash) |
| `--version` | Show version, libnfs version, and build info |

### Timeout and retry options

| Option | Default | Description |
|--------|---------|------------|
| `--timeout <ms>` | 10000 | RPC request timeout in milliseconds |
| `--retrans <n>` | 0 | RPC retransmission attempts per NFS call |
| `--autoreconnect <n>` | 0 | TCP reconnect attempts on disconnect (-1=infinite) |
| `--tcp-syncnt <n>` | (system) | TCP SYN retry count for connection establishment |
| `--poll-timeout <ms>` | (system) | Poll interval between response checks |
| `--dead-timeout <s>` | disabled | Kill mount after N seconds of unresponsive NFS server |
| `--nfs4-retries <n>` | 5 | Retry attempts for NFS4 transient errors (EXPIRED/GRACE/BADHANDLE) |
| `--nfs4-retry-wait <s>` | 30 | Seconds to wait between NFS4 retries |

### Reconnect options

Auto-reconnect on ESTALE and EIO is **enabled by default**. To disable:

| Option | Description |
|--------|------------|
| `--do-not-reconnect-on-stale` | Disable auto-reconnect on ESTALE |
| `--do-not-reconnect-on-io-error` | Disable auto-reconnect on EIO |

The legacy flags `--reconnect-on-stale` and `--reconnect-on-io-error` are accepted for backward compatibility (no-op since both are now default).

### FUSE options

These are passed directly to libfuse3:

| Option | Description |
|--------|------------|
| `-f` | Run in foreground (do not daemonize) |
| `-d` | Enable FUSE debug output |
| `-s` | Force single-threaded mode (automatic for NFSv4) |

## Dead-server watchdog (`--dead-timeout`)

The watchdog is a dedicated thread that runs independently of the FUSE event loop and NFS calls. It monitors two volatile timestamps without acquiring any mutexes:

1. **Stuck NFS call detection**: if an NFS call has been in-flight for >= dead-timeout seconds
2. **No-progress detection**: if no successful NFS operation has completed for >= dead-timeout seconds

When triggered:
1. Sets `dead_triggered=1` so all new FUSE operations return EIO immediately
2. Calls `fuse_exit()` to signal the FUSE event loop to stop
3. Calls `shutdown()` on the NFS TCP socket to unblock the stuck libnfs call
4. The stuck call returns error, the FUSE I/O path returns **EIO** (not ENOTCONN) to applications
5. Safety net: `_exit(1)` after 10 seconds if graceful shutdown doesn't complete

Applications (like QEMU) receive EIO which they handle properly, unlike ENOTCONN ("Transport endpoint is not connected") which causes freezes.

### Recommended settings

```bash
nfsfuse --dead-timeout 120 --timeout 15000 --retrans 5 \
  --autoreconnect 100 --nfs4-retries 5 --nfs4-retry-wait 30 \
  --log-errors --noatime --nodiratime --noexec \
  'nfs://server/export?version=4' /mnt/nfs
```

This gives:
- **75 seconds** max per NFS call (15s timeout x 5 retrans)
- **120 seconds** dead-timeout watchdog safety net
- **150 seconds** NFS4 retry window (5 retries x 30s for GRACE/EXPIRED recovery)
- Auto-reconnect on ESTALE, EIO, NFS4ERR_EXPIRED, NFS4ERR_GRACE, NFS4ERR_BADHANDLE

## NFS4 error handling

libnfs maps multiple NFS4 errors to the same errno values. nfsfuse handles all of them:

| NFS4 Error | libnfs errno | Action |
|-----------|-------------|--------|
| NFS4ERR_EXPIRED | ERANGE (-34) | Reconnect NFS session + retry |
| NFS4ERR_GRACE | ERANGE (-34) | Wait + retry (server recovering) |
| NFS4ERR_STALE_CLIENTID | ERANGE (-34) | Reconnect + retry |
| NFS4ERR_BADHANDLE | EINVAL (-22) | Reconnect + retry |
| ESTALE | ESTALE (-116) | Reconnect + retry (default on) |
| EIO | EIO (-5) | Reconnect + retry (default on) |
| ETIMEDOUT | ETIMEDOUT (-110) | Wait + retry |
| Context changed (async) | EAGAIN (-11) | Immediate retry with new context |

For metadata operations (getattr, open, mkdir, etc.), the `META_RETRY` macro handles reconnect + retry up to `--nfs4-retries` times. Up to 3 reconnects are allowed per retry loop — if the server flaps multiple times, nfsfuse reconnects each time rather than giving up after the first. For I/O operations (read, write), the retry loop sleeps `--nfs4-retry-wait` seconds between attempts, then reconnects after retries are exhausted. If a read/write file handle becomes stale after reconnect, it is transparently reopened (up to 5 times per operation). Flush and fsync operations also retry on transient errors.

For file-handle-based operations (getattr, truncate, chmod, chown), a transient NFS error on the file handle automatically falls through to the path-based retry path, which has full reconnect/recovery logic.

ERANGE and EAGAIN are mapped to EIO before returning to applications, so callers never see "Numerical result out of range" or spurious "Resource temporarily unavailable."

## Performance mode (`--max`)

### NFSv3 + `--max`

| Setting | Value |
|---------|-------|
| Page cache | `auto_cache` (invalidates on mtime/size change) |
| Attr timeout | 3 seconds |
| Entry timeout | 5 seconds |
| I/O chunk size | Up to 1 MB (capped by server readmax/writemax) |
| FUSE background requests | 64 |
| Async read | Enabled |
| Per-file NFS context | Yes (each file gets its own NFS connection) |
| FUSE threading | Multi-threaded |

### NFSv4 + `--max`

| Setting | Value |
|---------|-------|
| Page cache | Disabled (no kernel caching) |
| Attr/entry timeout | 0 (every access hits the server) |
| I/O chunk size | Up to 1 MB |
| FUSE background requests | 16 |
| Shared NFS context | Yes (all files share one connection, serialized by meta_lock) |
| FUSE threading | Single-threaded (forced by `-s`) |
| NFS4 autoreconnect | Capped to 3 (prevents silent state loss) |

### Without `--max`

Conservative settings: 128KB I/O chunks, `auto_cache` with 1s timeouts (NFSv3), no caching (NFSv4).

### `--writeback-cache`

Opt-in flag that enables `FUSE_CAP_WRITEBACK_CACHE` for both NFSv3 and NFSv4. The kernel buffers writes and flushes asynchronously, providing 2-5x write throughput improvement. **Risk**: unflushed writes are silently lost on crash or server disconnect. Only use for workloads where data loss is acceptable (e.g., backup jobs that can be re-run).

## Syslog logging (`--log-errors`)

Syslog messages include the mountpoint for easy identification when multiple mounts run on the same host:

```
nfsfuse[/storage/us-at-cdimage][1234]: NFS4ERR_EXPIRED on open /file — reconnecting
nfsfuse[/storage/us-at-cdimage][1234]: reconnected after NFS4ERR_EXPIRED, retrying open /file
nfsfuse[/storage/us-at-cdimage][1234]: open /file recovered after 1 retries
```

Retriable errors (ERANGE, ETIMEDOUT, ESTALE, EINVAL, EIO when reconnect-on-io-error is active) are **not** logged individually — only the retry lifecycle (retry N/M, recovered, or failed after N retries) is logged to avoid syslog spam.

Non-retriable errors are logged immediately at `ERR` priority.

## Deferred close pool

NFSv4 uses a deferred close pool (64 slots) that keeps recently closed file handles alive for up to 5 minutes. This prevents the NFS server from reclaiming resources while a file may still be needed (close-to-open consistency).

The pool is **flushed on reconnect** to prevent use-after-free of stale handles from dead sessions. All pool operations (add, release, expire, fstat, peek) validate that the stored context matches the current NFS session before use — stale entries from dead sessions are abandoned rather than risking a crash. Pool eviction under pressure uses non-blocking trylock to prevent mount freezes during high file turnover.

## NFSv4 notes

- NFSv4 automatically forces single-threaded FUSE (`-s`) because libnfs NFS contexts are not thread-safe
- All file operations share a single NFS context protected by `meta_lock`
- Directory caching is disabled for NFSv4 to avoid stale data
- NFS4 lease keepalive thread sends `getattr("/")` every 30 seconds
- Autoreconnect is capped to 3 for NFSv4 (prevents silent session state loss)
- On reconnect, the old NFS context's socket is shut down and closed to free the privileged port — prevents port exhaustion on systems with many reconnects
- Directory handles use `dir_handle_ctx()` to always reference the current NFS context after reconnect, preventing stale-context crashes
- Async NFS operations validate the context is still current before submitting requests and free resources immediately if the context was swapped during the operation

## Supported FUSE operations

| Operation | Description |
|-----------|--------------------------|
| getattr | Stat files and directories |
| access | Check file access permissions |
| readlink | Read symbolic link target |
| opendir / readdir / releasedir | List directory contents |
| open / create / release | Open, create, close files |
| mknod | Create device/special files |
| read / write | Read and write file data |
| truncate | Truncate files |
| utimens | Set file timestamps |
| chmod | Change file permissions |
| chown | Change file owner/group |
| unlink | Delete files |
| mkdir / rmdir | Create and remove directories |
| rename | Rename files and directories |
| symlink | Create symbolic links |
| link | Create hard links |
| fsync | Flush file data to server |
| statfs | Filesystem statistics |

## Examples

Mount an NFSv4 export with full resilience:
```bash
nfsfuse --dead-timeout 120 --timeout 15000 --retrans 5 \
  --autoreconnect 100 --log-errors \
  --noatime --nodiratime --noexec \
  'nfs://192.168.1.100/export/data?version=4' /mnt/nfs
```

Mount an NFSv3 export with performance mode:
```bash
nfsfuse --max --timeout 15000 --retrans 5 \
  --autoreconnect -1 --log-errors \
  'nfs://192.168.1.100/export/data?version=3' /mnt/nfs
```

Debug to a file:
```bash
nfsfuse --debug 2 --debug-output /var/log/nfsfuse.log \
  'nfs://10.0.0.1/data?version=4' /mnt/nfs
```

Debug to syslog:
```bash
nfsfuse --debug 2 --debug-output syslog 'nfs://10.0.0.1/data' /mnt/nfs
```

Run in foreground with FUSE debug output:
```bash
nfsfuse 'nfs://192.168.1.100/export/data' /mnt/nfs -f -d
```

Unmount:
```bash
fusermount3 -u /mnt/nfs
# or for a stale mount:
umount -l /mnt/nfs
```

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

### CI build

The GitHub Actions workflow automatically builds a fully static binary inside an Alpine container. It clones the latest stable [libnfs](https://github.com/sahlberg/libnfs) and [libfuse3](https://github.com/libfuse/libfuse) from source, compiles them as static libraries, and links everything into a single portable binary.

The libnfs version is controlled by the `LIBNFS_TAG` variable at the top of `.github/workflows/build.yml`:

```yaml
env:
  LIBNFS_TAG: libnfs-6.0.2
```

### Building locally with system packages

```bash
apt-get install gcc libfuse3-dev libnfs-dev pkg-config

gcc -O2 -Wall -o nfsfuse nfsfuse.c \
    $(pkg-config --cflags --libs fuse3 libnfs) \
    -lpthread
```

### Building locally with libnfs from source

```bash
apt-get install gcc cmake git pkg-config libfuse3-dev

LIBNFS_TAG=libnfs-6.0.2
git clone --depth 1 --branch "$LIBNFS_TAG" https://github.com/sahlberg/libnfs.git /tmp/libnfs
cd /tmp/libnfs && mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF
make -j$(nproc) && sudo make install

export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:$PKG_CONFIG_PATH
gcc -O2 -Wall -o nfsfuse nfsfuse.c \
    -DNFSFUSE_LIBNFS_VERSION="\"$LIBNFS_TAG\"" \
    $(pkg-config --cflags fuse3 libnfs) \
    $(pkg-config --libs --static fuse3 libnfs) \
    -lpthread
```

### Fully static build (portable binary)

```bash
docker run --rm -v "$PWD:/src" -w /src alpine:latest sh -exc '
  apk add --no-cache gcc musl-dev make cmake git meson ninja pkgconf linux-headers

  LIBNFS_TAG=libnfs-6.0.2

  git clone --depth 1 --branch "$LIBNFS_TAG" https://github.com/sahlberg/libnfs.git /tmp/libnfs
  cd /tmp/libnfs && mkdir build && cd build
  cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF
  make -j$(nproc) && make install

  git clone --depth 1 https://github.com/libfuse/libfuse.git /tmp/libfuse
  cd /tmp/libfuse && mkdir build && cd build
  meson setup .. --default-library=static --prefix=/usr -Dexamples=false -Dutils=false
  ninja && ninja install

  cd /src
  gcc -O2 -Wall -o nfsfuse nfsfuse.c -static \
      -DNFSFUSE_LIBNFS_VERSION="\"$LIBNFS_TAG\"" \
      $(pkg-config --cflags fuse3 libnfs) \
      $(pkg-config --libs --static fuse3 libnfs) \
      -lpthread
  strip nfsfuse
'
```

The resulting binary runs on any Linux with FUSE kernel support (Debian 8+, Ubuntu 14.04+, CentOS 7+, etc.).

## License

See source for details.
