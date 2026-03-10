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

### CI build

The GitHub Actions workflow automatically builds a fully static binary inside an Alpine container. It clones the latest stable [libnfs](https://github.com/sahlberg/libnfs) and [libfuse3](https://github.com/libfuse/libfuse) from source, compiles them as static libraries, and links everything into a single portable binary.

The libnfs version is controlled by the `LIBNFS_TAG` variable at the top of `.github/workflows/build.yml`:

```yaml
env:
  LIBNFS_TAG: libnfs-6.0.2
```

To change the libnfs version, update this tag to any release from the [libnfs releases page](https://github.com/sahlberg/libnfs/tags) (e.g. `libnfs-5.0.3`, `libnfs-6.0.1`). Push the change and CI will rebuild with that version.

### Building locally with system packages

If your distro ships `libnfs-dev` and `libfuse3-dev`:

```bash
# Debian/Ubuntu
apt-get install gcc libfuse3-dev libnfs-dev pkg-config

gcc -O2 -Wall -o nfsfuse nfsfuse.c \
    $(pkg-config --cflags --libs fuse3 libnfs) \
    -lpthread
```

### Building locally with libnfs from source

To use a specific libnfs version (recommended for best compatibility):

```bash
# Install build tools and libfuse3
apt-get install gcc cmake git pkg-config libfuse3-dev

# Clone and build libnfs (change the tag for a different version)
LIBNFS_TAG=libnfs-6.0.2
git clone --depth 1 --branch "$LIBNFS_TAG" https://github.com/sahlberg/libnfs.git /tmp/libnfs
cd /tmp/libnfs
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF
make -j$(nproc)
sudo make install

# Build nfsfuse
export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:$PKG_CONFIG_PATH
gcc -O2 -Wall -o nfsfuse nfsfuse.c \
    -DNFSFUSE_LIBNFS_VERSION="\"$LIBNFS_TAG\"" \
    $(pkg-config --cflags fuse3 libnfs) \
    $(pkg-config --libs --static fuse3 libnfs) \
    -lpthread
```

### Fully static build (portable binary)

To produce a single static binary with no runtime dependencies (like the CI does):

```bash
# Requires: gcc, cmake, git, meson, ninja, pkg-config, linux-headers, musl-dev (on Alpine)
# Or run inside the Alpine Docker container:
docker run --rm -v "$PWD:/src" -w /src alpine:latest sh -exc '
  apk add --no-cache gcc musl-dev make cmake git meson ninja pkgconf linux-headers

  LIBNFS_TAG=libnfs-6.0.2

  # Build libnfs
  git clone --depth 1 --branch "$LIBNFS_TAG" https://github.com/sahlberg/libnfs.git /tmp/libnfs
  cd /tmp/libnfs && mkdir build && cd build
  cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF
  make -j$(nproc) && make install

  # Build libfuse3
  git clone --depth 1 https://github.com/libfuse/libfuse.git /tmp/libfuse
  cd /tmp/libfuse && mkdir build && cd build
  meson setup .. --default-library=static --prefix=/usr -Dexamples=false -Dutils=false
  ninja && ninja install

  # Compile nfsfuse
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
