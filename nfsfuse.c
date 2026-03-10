#define FUSE_USE_VERSION 31

#ifndef NFSFUSE_VERSION
#define NFSFUSE_VERSION "0.0.0"
#endif
#ifndef NFSFUSE_BUILD
#define NFSFUSE_BUILD "0"
#endif
#ifndef NFSFUSE_LIBNFS_VERSION
#define NFSFUSE_LIBNFS_VERSION "unknown"
#endif
#ifndef NFSFUSE_BUILD_DATE
#define NFSFUSE_BUILD_DATE "unknown"
#endif
#ifndef NFSFUSE_BUILD_HOST
#define NFSFUSE_BUILD_HOST "unknown"
#endif

#include <fuse3/fuse.h>
#include <nfsc/libnfs.h>

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#ifndef LIBNFS_MKDIR_TAKES_MODE
#define LIBNFS_MKDIR_TAKES_MODE 0
#endif

#ifndef NFUSE_MAX_IO_CHUNK
#define NFUSE_MAX_IO_CHUNK (1024 * 1024)
#endif

#ifndef FUSE_CAP_ASYNC_READ
#define FUSE_CAP_ASYNC_READ 0
#endif
#ifndef FUSE_CAP_ATOMIC_O_TRUNC
#define FUSE_CAP_ATOMIC_O_TRUNC 0
#endif
#ifndef FUSE_CAP_WRITEBACK_CACHE
#define FUSE_CAP_WRITEBACK_CACHE 0
#endif

/*
 * Newer libnfs API from your static build:
 *   nfs_pread(ctx, fh, buf, count, offset)
 *   nfs_pwrite(ctx, fh, buf, count, offset)
 */
#define CALL_NFS_PREAD(ctx, fh, off, cnt, buf) \
    nfs_pread((ctx), (fh), (void *)(buf), (size_t)(cnt), (uint64_t)(off))
#define CALL_NFS_PWRITE(ctx, fh, off, cnt, buf) \
    nfs_pwrite((ctx), (fh), (const void *)(buf), (size_t)(cnt), (uint64_t)(off))

#if LIBNFS_MKDIR_TAKES_MODE
#define CALL_NFS_MKDIR(ctx, path, mode) nfs_mkdir((ctx), (path), (mode))
#else
#define CALL_NFS_MKDIR(ctx, path, mode) ((void)(mode), nfs_mkdir((ctx), (path)))
#endif

static int g_debug = 0;

#define DBG(...) do { if (g_debug) fprintf(stderr, __VA_ARGS__); } while (0)

static void print_version(void)
{
    fprintf(stderr, "nfsfuse %s (build %s)\n", NFSFUSE_VERSION, NFSFUSE_BUILD);
    fprintf(stderr, "  libnfs: %s\n", NFSFUSE_LIBNFS_VERSION);
    fprintf(stderr, "  built:  %s on %s\n", NFSFUSE_BUILD_DATE, NFSFUSE_BUILD_HOST);
}

struct app_state {
    char *url_base;
    char *url_effective;
    char *fsname;
    int max_mode;
    int safe_v4_mode;
    size_t io_chunk;
    uint32_t fuse_max_read;
    struct nfs_context *meta_nfs;
    pthread_mutex_t meta_lock;
    int meta_lock_init;
};

struct file_handle {
    struct nfs_context *ctx;
    struct nfsfh *fh;
    pthread_mutex_t lock;
    int own_ctx;
};

struct dir_entry {
    char *name;
    ino_t ino;
    mode_t mode;
};

struct dir_handle {
    struct nfs_context *ctx;
    struct nfsdir *dir;
    pthread_mutex_t lock;
    int own_ctx;

    struct dir_entry *entries;
    size_t entry_count;
    size_t entry_cap;
    int loaded;
};

static struct app_state g_state = {0};

static int nfs_err(int rc)
{
    if (rc < 0)
        return rc;
    return -EIO;
}

static char *xstrdup(const char *s)
{
    if (s == NULL)
        return NULL;
    return strdup(s);
}

static uint32_t clamp_u32_from_size(size_t v)
{
    if (v > 0xffffffffu)
        return 0xffffffffu;
    return (uint32_t)v;
}

static void destroy_nfs_context_safe(struct nfs_context *ctx)
{
    if (ctx)
        nfs_destroy_context(ctx);
}

static void fill_stat_from_nfs64(struct stat *stbuf, const struct nfs_stat_64 *st)
{
    memset(stbuf, 0, sizeof(*stbuf));

    stbuf->st_mode = (mode_t)st->nfs_mode;
    stbuf->st_nlink = (nlink_t)st->nfs_nlink;
    stbuf->st_uid = (uid_t)st->nfs_uid;
    stbuf->st_gid = (gid_t)st->nfs_gid;
    stbuf->st_rdev = (dev_t)st->nfs_rdev;
    stbuf->st_size = (off_t)st->nfs_size;
    stbuf->st_blocks = (blkcnt_t)st->nfs_blocks;
    stbuf->st_blksize = (blksize_t)st->nfs_blksize;
    stbuf->st_ino = (ino_t)st->nfs_ino;
    stbuf->st_dev = (dev_t)st->nfs_dev;

#if defined(__linux__)
    stbuf->st_atim.tv_sec = (time_t)st->nfs_atime;
    stbuf->st_atim.tv_nsec = (long)st->nfs_atime_nsec;
    stbuf->st_mtim.tv_sec = (time_t)st->nfs_mtime;
    stbuf->st_mtim.tv_nsec = (long)st->nfs_mtime_nsec;
    stbuf->st_ctim.tv_sec = (time_t)st->nfs_ctime;
    stbuf->st_ctim.tv_nsec = (long)st->nfs_ctime_nsec;
#endif
}

static int url_has_key(const char *url, const char *key)
{
    const char *q;
    size_t klen;

    if (url == NULL || key == NULL)
        return 0;

    q = strchr(url, '?');
    if (q == NULL)
        return 0;

    q++;
    klen = strlen(key);

    while (*q) {
        if (strncmp(q, key, klen) == 0 &&
            (q[klen] == '=' || q[klen] == '&' || q[klen] == '\0')) {
            return 1;
        }

        q = strchr(q, '&');
        if (q == NULL)
            break;
        q++;
    }

    return 0;
}

static int url_key_equals(const char *url, const char *key, const char *value)
{
    const char *q;
    size_t klen, vlen;

    if (url == NULL || key == NULL || value == NULL)
        return 0;

    q = strchr(url, '?');
    if (q == NULL)
        return 0;

    q++;
    klen = strlen(key);
    vlen = strlen(value);

    while (*q) {
        if (strncmp(q, key, klen) == 0 &&
            q[klen] == '=' &&
            strncmp(q + klen + 1, value, vlen) == 0 &&
            (q[klen + 1 + vlen] == '&' || q[klen + 1 + vlen] == '\0')) {
            return 1;
        }

        q = strchr(q, '&');
        if (q == NULL)
            break;
        q++;
    }

    return 0;
}

static char *url_append_opt(char *url, const char *opt)
{
    size_t len_url, len_opt;
    char sep;
    char *out;

    if (url == NULL || opt == NULL)
        return NULL;

    len_url = strlen(url);
    len_opt = strlen(opt);
    sep = strchr(url, '?') ? '&' : '?';

    out = malloc(len_url + 1 + len_opt + 1);
    if (out == NULL) {
        free(url);
        return NULL;
    }

    memcpy(out, url, len_url);
    out[len_url] = sep;
    memcpy(out + len_url + 1, opt, len_opt);
    out[len_url + 1 + len_opt] = '\0';

    free(url);
    return out;
}

static char *build_effective_url(const char *base_url, int max_mode)
{
    char *url;

    url = xstrdup(base_url);
    if (url == NULL)
        return NULL;

    if (url_key_equals(url, "version", "4")) {
        if (!url_has_key(url, "dircache")) {
            url = url_append_opt(url, "dircache=0");
            if (url == NULL)
                return NULL;
        }

        return url;
    }

    if (max_mode) {
        if (!url_has_key(url, "dircache")) {
            url = url_append_opt(url, "dircache=1");
            if (url == NULL)
                return NULL;
        }

        if (!url_has_key(url, "readdir-buffer")) {
            url = url_append_opt(url, "readdir-buffer=1048576,1048576");
            if (url == NULL)
                return NULL;
        }
    }

    return url;
}

static char *build_fsname_from_url(const char *url)
{
    struct nfs_context *ctx = NULL;
    struct nfs_url *nurl = NULL;
    const char *server;
    const char *path;
    int need_sep;
    size_t len;
    char *out = NULL;

    ctx = nfs_init_context();
    if (ctx == NULL)
        return NULL;

    nurl = nfs_parse_url_dir(ctx, url);
    if (nurl == NULL) {
        nfs_destroy_context(ctx);
        return NULL;
    }

    server = nurl->server ? nurl->server : "unknown";
    path = nurl->path ? nurl->path : "";
    need_sep = (path[0] != '/');

    len = strlen("nfsfuse:/") + strlen(server) + (size_t)need_sep + strlen(path) + 1;
    out = malloc(len);
    if (out != NULL)
        snprintf(out, len, "nfsfuse:/%s%s%s", server, need_sep ? "/" : "", path);

    nfs_destroy_url(nurl);
    nfs_destroy_context(ctx);
    return out;
}

static struct nfs_context *mount_new_context(const char *url)
{
    struct nfs_context *ctx = NULL;
    struct nfs_url *nurl = NULL;

    DBG("  nfs_init_context...\n");
    ctx = nfs_init_context();
    if (ctx == NULL)
        return NULL;

    DBG("  nfs_parse_url_dir...\n");
    nurl = nfs_parse_url_dir(ctx, url);
    if (nurl == NULL) {
        fprintf(stderr, "nfsfuse: url parse failed: %s\n", nfs_get_error(ctx));
        nfs_destroy_context(ctx);
        return NULL;
    }

    DBG("  server=%s path=%s\n",
        nurl->server ? nurl->server : "(null)",
        nurl->path ? nurl->path : "(null)");

    DBG("  nfs_mount...\n");
    if (nfs_mount(ctx, nurl->server, nurl->path) != 0) {
        fprintf(stderr, "nfsfuse: mount failed: %s\n", nfs_get_error(ctx));
        nfs_destroy_url(nurl);
        nfs_destroy_context(ctx);
        return NULL;
    }

    DBG("  mount ok\n");
    nfs_destroy_url(nurl);
    return ctx;
}

static void cleanup_app_state(void)
{
    if (g_state.meta_nfs) {
        nfs_destroy_context(g_state.meta_nfs);
        g_state.meta_nfs = NULL;
    }

    free(g_state.url_base);
    g_state.url_base = NULL;

    free(g_state.url_effective);
    g_state.url_effective = NULL;

    free(g_state.fsname);
    g_state.fsname = NULL;

    if (g_state.meta_lock_init) {
        pthread_mutex_destroy(&g_state.meta_lock);
        g_state.meta_lock_init = 0;
    }
}

static void file_handle_lock(struct file_handle *h)
{
    if (h->own_ctx)
        pthread_mutex_lock(&h->lock);
    else
        pthread_mutex_lock(&g_state.meta_lock);
}

static void file_handle_unlock(struct file_handle *h)
{
    if (h->own_ctx)
        pthread_mutex_unlock(&h->lock);
    else
        pthread_mutex_unlock(&g_state.meta_lock);
}

static void dir_handle_lock(struct dir_handle *h)
{
    if (h->own_ctx)
        pthread_mutex_lock(&h->lock);
    else
        pthread_mutex_lock(&g_state.meta_lock);
}

static void dir_handle_unlock(struct dir_handle *h)
{
    if (h->own_ctx)
        pthread_mutex_unlock(&h->lock);
    else
        pthread_mutex_unlock(&g_state.meta_lock);
}

static int sanitize_open_flags(int flags)
{
    int out = 0;
    int acc = flags & O_ACCMODE;

    out |= acc;

#ifdef O_APPEND
    out |= flags & O_APPEND;
#endif
#ifdef O_TRUNC
    out |= flags & O_TRUNC;
#endif
#ifdef O_SYNC
    out |= flags & O_SYNC;
#endif
#ifdef O_NOFOLLOW
    out |= flags & O_NOFOLLOW;
#endif

    if (g_state.max_mode && !g_state.safe_v4_mode) {
        if ((out & O_ACCMODE) == O_WRONLY) {
            out &= ~O_ACCMODE;
            out |= O_RDWR;
        }
#ifdef O_APPEND
        out &= ~O_APPEND;
#endif
    }

    return out;
}

static int open_file_handle_common(const char *path, int flags, mode_t mode,
                                   int create, struct file_handle **out)
{
    struct nfs_context *ctx = NULL;
    struct nfsfh *fh = NULL;
    struct file_handle *h = NULL;
    int rc;
    int use_private_ctx = g_state.max_mode && !g_state.safe_v4_mode;

    if (use_private_ctx) {
        ctx = mount_new_context(g_state.url_effective);
        if (ctx == NULL)
            return -EIO;
    } else {
        ctx = g_state.meta_nfs;
    }

    if (use_private_ctx) {
        if (create) {
            rc = nfs_creat(ctx, path, mode, &fh);
            if (rc >= 0) {
                nfs_close(ctx, fh);
                fh = NULL;
                rc = nfs_open(ctx, path, flags, &fh);
            }
        } else {
            rc = nfs_open(ctx, path, flags, &fh);
        }
    } else {
        pthread_mutex_lock(&g_state.meta_lock);
        if (create) {
            rc = nfs_creat(ctx, path, mode, &fh);
            if (rc >= 0) {
                nfs_close(ctx, fh);
                fh = NULL;
                rc = nfs_open(ctx, path, flags, &fh);
            }
        } else {
            rc = nfs_open(ctx, path, flags, &fh);
        }
        pthread_mutex_unlock(&g_state.meta_lock);
    }

    if (rc < 0) {
        if (use_private_ctx)
            destroy_nfs_context_safe(ctx);
        return nfs_err(rc);
    }

    h = calloc(1, sizeof(*h));
    if (h == NULL) {
        if (use_private_ctx)
            nfs_close(ctx, fh);
        else {
            pthread_mutex_lock(&g_state.meta_lock);
            nfs_close(ctx, fh);
            pthread_mutex_unlock(&g_state.meta_lock);
        }

        if (use_private_ctx)
            destroy_nfs_context_safe(ctx);

        return -ENOMEM;
    }

    h->ctx = ctx;
    h->fh = fh;
    h->own_ctx = use_private_ctx;

    if (pthread_mutex_init(&h->lock, NULL) != 0) {
        if (use_private_ctx)
            nfs_close(ctx, fh);
        else {
            pthread_mutex_lock(&g_state.meta_lock);
            nfs_close(ctx, fh);
            pthread_mutex_unlock(&g_state.meta_lock);
        }

        if (use_private_ctx)
            destroy_nfs_context_safe(ctx);

        free(h);
        return -ENOMEM;
    }

    *out = h;
    return 0;
}

static int pread_full(struct file_handle *h, char *buf, size_t size, off_t offset)
{
    size_t done = 0;
    int rc = 0;

    file_handle_lock(h);

    while (done < size) {
        size_t chunk = size - done;

        if (g_state.max_mode && !g_state.safe_v4_mode && chunk > g_state.io_chunk)
            chunk = g_state.io_chunk;

        rc = CALL_NFS_PREAD(h->ctx, h->fh, offset + (off_t)done, chunk, buf + done);
        if (rc < 0) {
            if (done == 0) {
                file_handle_unlock(h);
                return nfs_err(rc);
            }
            break;
        }

        if (rc == 0)
            break;

        done += (size_t)rc;

        if ((size_t)rc < chunk)
            break;
    }

    file_handle_unlock(h);
    return (int)done;
}

static int pwrite_full(struct file_handle *h, const char *buf, size_t size, off_t offset)
{
    size_t done = 0;
    int rc = 0;

    file_handle_lock(h);

    while (done < size) {
        size_t chunk = size - done;

        if (g_state.max_mode && !g_state.safe_v4_mode && chunk > g_state.io_chunk)
            chunk = g_state.io_chunk;

        rc = CALL_NFS_PWRITE(h->ctx, h->fh, offset + (off_t)done, chunk, buf + done);
        if (rc < 0) {
            if (done == 0) {
                file_handle_unlock(h);
                return nfs_err(rc);
            }
            break;
        }

        if (rc == 0) {
            file_handle_unlock(h);
            return (done > 0) ? (int)done : -EIO;
        }

        done += (size_t)rc;
    }

    file_handle_unlock(h);
    return (int)done;
}

static void dir_snapshot_free(struct dir_handle *h)
{
    size_t i;

    if (h == NULL)
        return;

    for (i = 0; i < h->entry_count; i++)
        free(h->entries[i].name);

    free(h->entries);
    h->entries = NULL;
    h->entry_count = 0;
    h->entry_cap = 0;
    h->loaded = 0;
}

static int dir_snapshot_push(struct dir_handle *h, const struct nfsdirent *ent)
{
    struct dir_entry *tmp;
    char *name;

    if (h == NULL || ent == NULL || ent->name == NULL)
        return 0;

    if (strcmp(ent->name, ".") == 0 || strcmp(ent->name, "..") == 0)
        return 0;

    if (h->entry_count == h->entry_cap) {
        size_t new_cap = h->entry_cap ? h->entry_cap * 2 : 256;

        tmp = realloc(h->entries, new_cap * sizeof(*tmp));
        if (tmp == NULL)
            return -ENOMEM;

        h->entries = tmp;
        h->entry_cap = new_cap;
    }

    name = strdup(ent->name);
    if (name == NULL)
        return -ENOMEM;

    h->entries[h->entry_count].name = name;
    h->entries[h->entry_count].ino = (ino_t)ent->inode;
    h->entries[h->entry_count].mode = (mode_t)ent->mode;
    h->entry_count++;

    return 0;
}

static int dir_snapshot_load(struct dir_handle *h)
{
    struct nfsdirent *ent;
    int rc = 0;

    if (h == NULL)
        return -EBADF;

    if (h->loaded)
        return 0;

    dir_handle_lock(h);
    nfs_rewinddir(h->ctx, h->dir);

    while ((ent = nfs_readdir(h->ctx, h->dir)) != NULL) {
        rc = dir_snapshot_push(h, ent);
        if (rc < 0)
            break;
    }

    dir_handle_unlock(h);

    if (rc < 0) {
        dir_snapshot_free(h);
        return rc;
    }

    h->loaded = 1;
    return 0;
}

static int nfuse_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi)
{
    struct nfs_stat_64 st;
    int rc;

    if (fi && fi->fh) {
        struct file_handle *h = (struct file_handle *)(uintptr_t)fi->fh;

        file_handle_lock(h);
        rc = nfs_fstat64(h->ctx, h->fh, &st);
        file_handle_unlock(h);
    } else {
        if (path == NULL)
            return -EINVAL;

        pthread_mutex_lock(&g_state.meta_lock);
        rc = nfs_lstat64(g_state.meta_nfs, path, &st);
        pthread_mutex_unlock(&g_state.meta_lock);
    }

    if (rc < 0)
        return nfs_err(rc);

    fill_stat_from_nfs64(stbuf, &st);
    return 0;
}

static int nfuse_opendir(const char *path, struct fuse_file_info *fi)
{
    struct dir_handle *h = NULL;
    struct nfs_context *ctx = NULL;
    struct nfsdir *dir = NULL;
    int rc;
    int use_private_ctx = g_state.max_mode && !g_state.safe_v4_mode;

    h = calloc(1, sizeof(*h));
    if (h == NULL)
        return -ENOMEM;

    if (use_private_ctx) {
        ctx = mount_new_context(g_state.url_effective);
        if (ctx == NULL) {
            free(h);
            return -EIO;
        }

        rc = nfs_opendir(ctx, path, &dir);
        if (rc < 0) {
            destroy_nfs_context_safe(ctx);
            free(h);
            return nfs_err(rc);
        }

        h->ctx = ctx;
        h->own_ctx = 1;
    } else {
        pthread_mutex_lock(&g_state.meta_lock);
        rc = nfs_opendir(g_state.meta_nfs, path, &dir);
        pthread_mutex_unlock(&g_state.meta_lock);

        if (rc < 0) {
            free(h);
            return nfs_err(rc);
        }

        h->ctx = g_state.meta_nfs;
        h->own_ctx = 0;
    }

    if (pthread_mutex_init(&h->lock, NULL) != 0) {
        if (h->own_ctx) {
            nfs_closedir(h->ctx, dir);
            destroy_nfs_context_safe(h->ctx);
        } else {
            pthread_mutex_lock(&g_state.meta_lock);
            nfs_closedir(h->ctx, dir);
            pthread_mutex_unlock(&g_state.meta_lock);
        }

        free(h);
        return -ENOMEM;
    }

    h->dir = dir;
    h->entries = NULL;
    h->entry_count = 0;
    h->entry_cap = 0;
    h->loaded = 0;

    fi->fh = (uint64_t)(uintptr_t)h;
    return 0;
}

static int nfuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t off, struct fuse_file_info *fi,
                         enum fuse_readdir_flags flags)
{
    struct dir_handle *h = (struct dir_handle *)(uintptr_t)fi->fh;
    size_t i, start;
    int rc;

    (void)path;
    (void)flags;

    if (h == NULL)
        return -EBADF;

    if (off < 0)
        return -EINVAL;

    rc = dir_snapshot_load(h);
    if (rc < 0)
        return rc;

    if (off == 0) {
        if (filler(buf, ".", NULL, 1, 0) != 0)
            return 0;
        off = 1;
    }

    if (off == 1) {
        if (filler(buf, "..", NULL, 2, 0) != 0)
            return 0;
        off = 2;
    }

    if (off < 2)
        off = 2;

    start = (size_t)(off - 2);

    for (i = start; i < h->entry_count; i++) {
        struct stat st;

        memset(&st, 0, sizeof(st));
        st.st_ino = h->entries[i].ino;
        st.st_mode = h->entries[i].mode;

        if (filler(buf, h->entries[i].name, &st, (off_t)(i + 3), 0) != 0)
            break;
    }

    return 0;
}

static int nfuse_releasedir(const char *path, struct fuse_file_info *fi)
{
    struct dir_handle *h = (struct dir_handle *)(uintptr_t)fi->fh;

    (void)path;

    if (h == NULL)
        return 0;

    dir_snapshot_free(h);

    dir_handle_lock(h);
    nfs_closedir(h->ctx, h->dir);
    dir_handle_unlock(h);

    if (h->own_ctx)
        destroy_nfs_context_safe(h->ctx);

    pthread_mutex_destroy(&h->lock);
    free(h);
    fi->fh = 0;
    return 0;
}

static int nfuse_open(const char *path, struct fuse_file_info *fi)
{
    struct file_handle *h = NULL;
    int flags = sanitize_open_flags(fi->flags);
    int rc;

    rc = open_file_handle_common(path, flags, 0, 0, &h);
    if (rc < 0)
        return rc;

    fi->fh = (uint64_t)(uintptr_t)h;

    if (g_state.max_mode && !g_state.safe_v4_mode)
        fi->keep_cache = 1;
    else
        fi->keep_cache = 0;

#ifdef O_DIRECT
    if (fi->flags & O_DIRECT)
        fi->direct_io = 1;
#endif

    return 0;
}

static int nfuse_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    struct file_handle *h = NULL;
    int flags = sanitize_open_flags(fi->flags);
    int rc;

    rc = open_file_handle_common(path, flags, mode, 1, &h);
    if (rc < 0)
        return rc;

    fi->fh = (uint64_t)(uintptr_t)h;

    if (g_state.max_mode && !g_state.safe_v4_mode)
        fi->keep_cache = 1;
    else
        fi->keep_cache = 0;

#ifdef O_DIRECT
    if (fi->flags & O_DIRECT)
        fi->direct_io = 1;
#endif

    return 0;
}

static int nfuse_release(const char *path, struct fuse_file_info *fi)
{
    struct file_handle *h = (struct file_handle *)(uintptr_t)fi->fh;

    (void)path;

    if (h == NULL)
        return 0;

    file_handle_lock(h);
    nfs_close(h->ctx, h->fh);
    file_handle_unlock(h);

    if (h->own_ctx)
        destroy_nfs_context_safe(h->ctx);

    pthread_mutex_destroy(&h->lock);
    free(h);
    fi->fh = 0;
    return 0;
}

static int nfuse_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi)
{
    struct file_handle *h = (struct file_handle *)(uintptr_t)fi->fh;

    (void)path;

    if (h == NULL)
        return -EBADF;

    return pread_full(h, buf, size, offset);
}

static int nfuse_write(const char *path, const char *buf, size_t size, off_t offset,
                       struct fuse_file_info *fi)
{
    struct file_handle *h = (struct file_handle *)(uintptr_t)fi->fh;

    (void)path;

    if (h == NULL)
        return -EBADF;

    return pwrite_full(h, buf, size, offset);
}

static int nfuse_truncate(const char *path, off_t size, struct fuse_file_info *fi)
{
    int rc;

    if (fi && fi->fh) {
        struct file_handle *h = (struct file_handle *)(uintptr_t)fi->fh;

        file_handle_lock(h);
        rc = nfs_ftruncate(h->ctx, h->fh, (uint64_t)size);
        file_handle_unlock(h);
    } else {
        if (path == NULL)
            return -EINVAL;

        pthread_mutex_lock(&g_state.meta_lock);
        rc = nfs_truncate(g_state.meta_nfs, path, (uint64_t)size);
        pthread_mutex_unlock(&g_state.meta_lock);
    }

    if (rc < 0)
        return nfs_err(rc);

    return 0;
}

static int nfuse_utimens(const char *path, const struct timespec tv[2],
                         struct fuse_file_info *fi)
{
    (void)path;
    (void)tv;
    (void)fi;
    return 0;
}

static int nfuse_unlink(const char *path)
{
    int rc;

    pthread_mutex_lock(&g_state.meta_lock);
    rc = nfs_unlink(g_state.meta_nfs, path);
    pthread_mutex_unlock(&g_state.meta_lock);

    if (rc < 0)
        return nfs_err(rc);

    return 0;
}

static int nfuse_mkdir(const char *path, mode_t mode)
{
    int rc;

    (void)mode;

    pthread_mutex_lock(&g_state.meta_lock);
    rc = CALL_NFS_MKDIR(g_state.meta_nfs, path, mode);
    pthread_mutex_unlock(&g_state.meta_lock);

    if (rc < 0)
        return nfs_err(rc);

    return 0;
}

static int nfuse_rmdir(const char *path)
{
    int rc;

    pthread_mutex_lock(&g_state.meta_lock);
    rc = nfs_rmdir(g_state.meta_nfs, path);
    pthread_mutex_unlock(&g_state.meta_lock);

    if (rc < 0)
        return nfs_err(rc);

    return 0;
}

static int nfuse_rename(const char *from, const char *to, unsigned int flags)
{
    int rc;

    if (flags != 0)
        return -EINVAL;

    pthread_mutex_lock(&g_state.meta_lock);
    rc = nfs_rename(g_state.meta_nfs, from, to);
    pthread_mutex_unlock(&g_state.meta_lock);

    if (rc < 0)
        return nfs_err(rc);

    return 0;
}

static int nfuse_flush(const char *path, struct fuse_file_info *fi)
{
    (void)path;
    (void)fi;
    return 0;
}

static int nfuse_fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
    (void)path;
    (void)datasync;
    (void)fi;
    return 0;
}

static int nfuse_statfs(const char *path, struct statvfs *st)
{
    int rc;

    (void)path;

    memset(st, 0, sizeof(*st));

    pthread_mutex_lock(&g_state.meta_lock);
    rc = nfs_statvfs(g_state.meta_nfs, "/", st);
    pthread_mutex_unlock(&g_state.meta_lock);

    if (rc < 0)
        return nfs_err(rc);

    return 0;
}

static void *nfuse_init(struct fuse_conn_info *conn, struct fuse_config *cfg)
{
    cfg->use_ino = 1;
    cfg->readdir_ino = 1;

    if (g_state.safe_v4_mode) {
        cfg->kernel_cache = 0;
        cfg->auto_cache = 0;
        cfg->attr_timeout = 0.0;
        cfg->entry_timeout = 0.0;
        cfg->negative_timeout = 0.0;

        if (g_state.max_mode) {
            conn->max_read = g_state.fuse_max_read;

            if (conn->max_write == 0 || g_state.fuse_max_read < conn->max_write)
                conn->max_write = g_state.fuse_max_read;

            if (conn->max_readahead == 0 || g_state.fuse_max_read < conn->max_readahead)
                conn->max_readahead = g_state.fuse_max_read;

            if (conn->max_background == 0 || conn->max_background > 16)
                conn->max_background = 16;

            if (conn->congestion_threshold == 0 || conn->congestion_threshold > 12)
                conn->congestion_threshold = 12;
        }

        return NULL;
    }

    if (g_state.max_mode) {
        cfg->kernel_cache = 1;
        cfg->attr_timeout = 30.0;
        cfg->entry_timeout = 30.0;
        cfg->negative_timeout = 5.0;

        conn->max_read = g_state.fuse_max_read;

        if (conn->max_write == 0 || g_state.fuse_max_read < conn->max_write)
            conn->max_write = g_state.fuse_max_read;

        if (conn->max_readahead == 0 || g_state.fuse_max_read < conn->max_readahead)
            conn->max_readahead = g_state.fuse_max_read;

        if (conn->max_background == 0 || conn->max_background > 64)
            conn->max_background = 64;

        if (conn->congestion_threshold == 0 || conn->congestion_threshold > 48)
            conn->congestion_threshold = 48;

        if (conn->capable & FUSE_CAP_ASYNC_READ)
            conn->want |= FUSE_CAP_ASYNC_READ;
        if (conn->capable & FUSE_CAP_ATOMIC_O_TRUNC)
            conn->want |= FUSE_CAP_ATOMIC_O_TRUNC;
        if (conn->capable & FUSE_CAP_WRITEBACK_CACHE)
            conn->want |= FUSE_CAP_WRITEBACK_CACHE;
    } else {
        cfg->auto_cache = 1;
        cfg->ac_attr_timeout_set = 1;
        cfg->ac_attr_timeout = 1.0;
        cfg->attr_timeout = 1.0;
        cfg->entry_timeout = 1.0;
        cfg->negative_timeout = 0.0;
    }

    return NULL;
}

static void nfuse_destroy(void *private_data)
{
    (void)private_data;
    cleanup_app_state();
}

static struct fuse_operations nfuse_ops = {
    .init       = nfuse_init,
    .destroy    = nfuse_destroy,

    .getattr    = nfuse_getattr,

    .opendir    = nfuse_opendir,
    .readdir    = nfuse_readdir,
    .releasedir = nfuse_releasedir,

    .open       = nfuse_open,
    .create     = nfuse_create,
    .release    = nfuse_release,

    .read       = nfuse_read,
    .write      = nfuse_write,
    .truncate   = nfuse_truncate,
    .utimens    = nfuse_utimens,

    .unlink     = nfuse_unlink,
    .mkdir      = nfuse_mkdir,
    .rmdir      = nfuse_rmdir,
    .rename     = nfuse_rename,

    .flush      = nfuse_flush,
    .fsync      = nfuse_fsync,
    .statfs     = nfuse_statfs,
};

static void usage(const char *prog)
{
    print_version();
    fprintf(stderr,
        "\nUsage:\n"
        "  %s [--max] [--debug] nfs://server/export/path[?version=3|4] <mountpoint> [FUSE options]\n\n"
        "Options:\n"
        "  --max      Enable performance optimizations\n"
        "  --debug    Print debug tracing to stderr\n"
        "  --version  Show version information\n\n"
        "Examples:\n"
        "  %s 'nfs://192.168.52.200/store001/cdimage?version=3' /mnt/nfs\n"
        "  %s --max 'nfs://192.168.52.200/store001/cdimage?version=4' /mnt/nfs\n"
        "  %s --debug 'nfs://192.168.52.200/store001/cdimage?version=4' /mnt/nfs\n",
        prog, prog, prog, prog);
}

static int add_fuse_arg(char ***argvp, int *argcp, const char *arg)
{
    char **tmp;
    char *copy;

    copy = xstrdup(arg);
    if (copy == NULL)
        return -1;

    tmp = realloc(*argvp, sizeof(char *) * (size_t)(*argcp + 1));
    if (tmp == NULL) {
        free(copy);
        return -1;
    }

    *argvp = tmp;
    (*argvp)[*argcp] = copy;
    (*argcp)++;
    return 0;
}

static int append_fsname_mount_option(char ***argvp, int *argcp)
{
    size_t len;
    char *opt;
    int rc = -1;

    if (g_state.fsname == NULL)
        return 0;

    len = strlen("fsname=") + strlen(g_state.fsname) + 1;
    opt = malloc(len);
    if (opt == NULL)
        return -1;

    snprintf(opt, len, "fsname=%s", g_state.fsname);

    if (add_fuse_arg(argvp, argcp, "-o") != 0)
        goto out;
    if (add_fuse_arg(argvp, argcp, opt) != 0)
        goto out;

    rc = 0;
out:
    free(opt);
    return rc;
}

static int append_max_mount_options(char ***argvp, int *argcp)
{
    char opt[64];

    snprintf(opt, sizeof(opt), "max_read=%u", g_state.fuse_max_read);

    if (add_fuse_arg(argvp, argcp, "-o") != 0)
        return -1;
    if (add_fuse_arg(argvp, argcp, opt) != 0)
        return -1;

    return 0;
}

static int user_passed_single_thread(int argc, char *argv[])
{
    int i;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-s") == 0)
            return 1;
    }

    return 0;
}

static void free_fuse_args(char **argv, int argc)
{
    int i;

    if (argv == NULL)
        return;

    for (i = 0; i < argc; i++)
        free(argv[i]);

    free(argv);
}

int main(int argc, char *argv[])
{
    int i;
    int url_idx = -1;
    int mount_idx = -1;
    int fuse_argc = 0;
    char **fuse_argv = NULL;
    int rc = 1;
    size_t readmax = 0;
    size_t writemax = 0;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        }
        if (strcmp(argv[i], "--version") == 0) {
            print_version();
            return 0;
        }
        if (strcmp(argv[i], "--debug") == 0)
            g_debug = 1;
    }

    if (argc < 3) {
        usage(argv[0]);
        return 1;
    }

    print_version();

    if (pthread_mutex_init(&g_state.meta_lock, NULL) != 0) {
        fprintf(stderr, "pthread_mutex_init failed\n");
        return 1;
    }
    g_state.meta_lock_init = 1;
    g_state.io_chunk = 128 * 1024;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--max") == 0) {
            g_state.max_mode = 1;
            continue;
        }
        if (strcmp(argv[i], "--debug") == 0)
            continue;

        if (url_idx == -1) {
            url_idx = i;
            continue;
        }

        if (mount_idx == -1) {
            mount_idx = i;
            continue;
        }
    }

    if (url_idx == -1 || mount_idx == -1) {
        usage(argv[0]);
        cleanup_app_state();
        return 1;
    }

    g_state.url_base = xstrdup(argv[url_idx]);
    if (g_state.url_base == NULL) {
        cleanup_app_state();
        return 1;
    }

    g_state.safe_v4_mode = url_key_equals(g_state.url_base, "version", "4");

    g_state.url_effective = build_effective_url(g_state.url_base, g_state.max_mode);
    if (g_state.url_effective == NULL) {
        cleanup_app_state();
        return 1;
    }

    DBG("nfsfuse: url=%s v4=%d max=%d\n",
        g_state.url_effective, g_state.safe_v4_mode, g_state.max_mode);

    g_state.fsname = build_fsname_from_url(g_state.url_base);
    if (g_state.fsname == NULL)
        g_state.fsname = xstrdup("nfsfuse");

    DBG("nfsfuse: mounting...\n");

    g_state.meta_nfs = mount_new_context(g_state.url_effective);
    if (g_state.meta_nfs == NULL) {
        fprintf(stderr, "nfsfuse: mount failed for %s\n", g_state.url_effective);
        cleanup_app_state();
        return 1;
    }

    readmax = nfs_get_readmax(g_state.meta_nfs);
    writemax = nfs_get_writemax(g_state.meta_nfs);

    DBG("nfsfuse: mounted, readmax=%zu writemax=%zu\n", readmax, writemax);

    if (g_state.max_mode) {
        g_state.io_chunk = NFUSE_MAX_IO_CHUNK;
        if (readmax > 0 && readmax < g_state.io_chunk)
            g_state.io_chunk = readmax;
        if (writemax > 0 && writemax < g_state.io_chunk)
            g_state.io_chunk = writemax;
        if (g_state.io_chunk < (128 * 1024))
            g_state.io_chunk = 128 * 1024;
    }

    g_state.fuse_max_read = clamp_u32_from_size(g_state.io_chunk);

    if (add_fuse_arg(&fuse_argv, &fuse_argc, argv[0]) != 0) {
        cleanup_app_state();
        return 1;
    }

    if (append_fsname_mount_option(&fuse_argv, &fuse_argc) != 0) {
        free_fuse_args(fuse_argv, fuse_argc);
        cleanup_app_state();
        return 1;
    }

    if (g_state.max_mode) {
        if (append_max_mount_options(&fuse_argv, &fuse_argc) != 0) {
            free_fuse_args(fuse_argv, fuse_argc);
            cleanup_app_state();
            return 1;
        }
    }

    if (g_state.safe_v4_mode && !user_passed_single_thread(argc, argv)) {
        if (add_fuse_arg(&fuse_argv, &fuse_argc, "-s") != 0) {
            free_fuse_args(fuse_argv, fuse_argc);
            cleanup_app_state();
            return 1;
        }
    }

    for (i = 1; i < argc; i++) {
        if (i == url_idx || i == mount_idx)
            continue;
        if (strcmp(argv[i], "--max") == 0 || strcmp(argv[i], "--debug") == 0)
            continue;

        if (add_fuse_arg(&fuse_argv, &fuse_argc, argv[i]) != 0) {
            free_fuse_args(fuse_argv, fuse_argc);
            cleanup_app_state();
            return 1;
        }
    }

    if (add_fuse_arg(&fuse_argv, &fuse_argc, argv[mount_idx]) != 0) {
        free_fuse_args(fuse_argv, fuse_argc);
        cleanup_app_state();
        return 1;
    }

    DBG("nfsfuse: starting fuse (argc=%d)\n", fuse_argc);
    for (i = 0; i < fuse_argc && g_debug; i++)
        DBG("  argv[%d]=%s\n", i, fuse_argv[i]);

    rc = fuse_main(fuse_argc, fuse_argv, &nfuse_ops, NULL);

    free_fuse_args(fuse_argv, fuse_argc);

    if (g_state.meta_nfs != NULL || g_state.url_base != NULL ||
        g_state.url_effective != NULL || g_state.fsname != NULL)
        cleanup_app_state();

    return rc;
}
