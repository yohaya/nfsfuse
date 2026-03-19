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
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <syslog.h>
#include <poll.h>
#include <sys/time.h>
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
static int g_log_errors = 0;
static int g_syslog_open = 0;
static char g_syslog_ident[256];  /* "nfsfuse[mountpoint]" for syslog */
static const char *g_mountpoint = "";  /* mountpoint path for debug log prefix */
static int g_noatime = 0;
static int g_nodiratime = 0;
static int g_noexec = 0;
static int g_reconnect_on_stale = 1;     /* on by default */
static int g_reconnect_on_io_error = 1;  /* on by default */
static int g_writeback_cache = 0;
static int g_async_mode = 0;     /* 1 = use async libnfs API with event loop */
static int g_auto_remount = 0;   /* 1 = re-exec on dead-timeout instead of exit */
static int g_saved_argc = 0;
static char **g_saved_argv = NULL;
static FILE *g_debug_file = NULL;  /* non-NULL = write debug to file */
static int g_debug_syslog = 0;    /* 1 = write debug to syslog */

static void dbg_print_timestamp(FILE *fp)
{
    struct timeval tv;
    struct tm tm;
    gettimeofday(&tv, NULL);
    localtime_r(&tv.tv_sec, &tm);
    if (g_mountpoint[0])
        fprintf(fp, "[%04d-%02d-%02d %02d:%02d:%02d.%03d] [%s] ",
                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                tm.tm_hour, tm.tm_min, tm.tm_sec,
                (int)(tv.tv_usec / 1000), g_mountpoint);
    else
        fprintf(fp, "[%04d-%02d-%02d %02d:%02d:%02d.%03d] ",
                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                tm.tm_hour, tm.tm_min, tm.tm_sec,
                (int)(tv.tv_usec / 1000));
}

#define DBG(lvl, ...) do { \
    if (g_debug >= (lvl)) { \
        if (g_debug_syslog) \
            syslog(LOG_DEBUG, __VA_ARGS__); \
        else { \
            FILE *_dbg_fp = g_debug_file ? g_debug_file : stderr; \
            dbg_print_timestamp(_dbg_fp); \
            fprintf(_dbg_fp, __VA_ARGS__); \
        } \
    } \
} while (0)

static void log_nfs_error(const char *op, const char *path,
                          int rc, struct nfs_context *ctx)
{
    const char *nfs_msg;

    nfs_msg = ctx ? nfs_get_error(ctx) : NULL;
    if (nfs_msg == NULL || nfs_msg[0] == '\0')
        nfs_msg = "unknown error";

    /* ENOENT is normal (file existence checks) — debug only */
    if (rc == -ENOENT) {
        DBG(2, "nfsfuse: %s %s: %s (rc=%d/%s)\n",
            op, path ? path : "", nfs_msg, rc, strerror(-rc));
        return;
    }

    /*
     * Retriable errors (ERANGE=NFS4ERR_EXPIRED/GRACE, ETIMEDOUT,
     * ESTALE) are logged by the retry loop itself (META_RETRY,
     * pread_full, pwrite_full) with retry counts and recovery status.
     * Don't duplicate them here — only log non-retriable errors.
     */
    if (rc == -ERANGE || rc == -ETIMEDOUT || rc == -ESTALE || rc == -EINVAL ||
        (g_reconnect_on_io_error && rc == -EIO)) {
        DBG(1, "nfsfuse: %s %s: %s (rc=%d/%s) [will retry]\n",
            op, path ? path : "", nfs_msg, rc, strerror(-rc));
        return;
    }

    if (!g_log_errors)
        return;

    syslog(LOG_ERR, "%s %s: %s (rc=%d/%s)",
           op, path ? path : "",
           nfs_msg, rc, strerror(-rc));
}

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

    int timeout;
    int retrans;
    int autoreconnect;
    int tcp_syncnt;
    int poll_timeout;

    int has_timeout;
    int has_retrans;
    int has_autoreconnect;
    int has_tcp_syncnt;
    int has_poll_timeout;

    /* NFS4 lease keepalive thread */
    pthread_t keepalive_thread;
    int keepalive_running;

    /* Dead-server watchdog: unmount after N seconds of consecutive failures */
    int dead_timeout;        /* 0 = disabled */
    int has_dead_timeout;
    long long first_failure_ms;  /* millisecond time of first consecutive failure, 0 = healthy */
    int dead_triggered;      /* 1 = already triggered unmount */

    /* Stuck-call recovery: shut down socket after N seconds of meta_lock busy */
    int stuck_timeout;       /* 0 = disabled */
    volatile time_t meta_lock_busy_since;  /* when trylock first failed, 0 = not busy */

    /*
     * Lock-free timestamps for the dedicated watchdog thread.
     * volatile ensures visibility across threads without a mutex.
     * Updated by I/O threads; read by the watchdog thread.
     */
    volatile time_t last_nfs_success;  /* last successful NFS op, 0 = none yet */
    volatile time_t nfs_call_start;    /* when current NFS call started, 0 = idle */

    pthread_t watchdog_thread;
    int watchdog_running;
};

struct file_handle {
    struct nfs_context *ctx;
    struct nfsfh *fh;
    pthread_mutex_t lock;
    int own_ctx;
    int writable;
    int borrowed;  /* 1 = NFS handle is owned by deferred pool, don't close */
};

/*
 * Deferred close pool.  Mimics the kernel NFS client's close-to-open
 * consistency: after an application closes a file, we keep the NFS4
 * OPEN state alive for up to DEFERRED_CLOSE_SEC seconds.  This prevents
 * the NFS server from evicting the file from its name cache or
 * reclaiming resources while the file may still be needed.
 *
 * The ISO disappearance bug: cwmsrv creates two ISOs (phase 0 and
 * phase 1), writes them, and closes both.  Phase 0 runs for ~3 minutes.
 * When phase 1 starts and tries to mount the phase 1 ISO, the server
 * returns NOENT — the file vanished after being closed.  By keeping the
 * NFS4 OPEN state alive, the server is forced to keep the file accessible.
 */
#define DEFERRED_CLOSE_MAX  64
#define DEFERRED_CLOSE_SEC  300  /* 5 minutes */

struct deferred_close_entry {
    char path[4096];
    struct nfs_context *ctx;
    struct nfsfh *fh;
    time_t created;
    int active;
};

static struct deferred_close_entry g_deferred[DEFERRED_CLOSE_MAX];
static pthread_mutex_t g_deferred_lock = PTHREAD_MUTEX_INITIALIZER;

/* Forward declarations — implementations after g_state */
static void deferred_close_add(const char *path, struct nfs_context *ctx,
                               struct nfsfh *fh);
static void deferred_close_release(const char *path);
static void deferred_close_expire(void);
static void deferred_close_all(void);
static int deferred_close_fstat(const char *path, struct nfs_stat_64 *st);
static int deferred_close_peek(const char *path, struct nfs_context **out_ctx,
                               struct nfsfh **out_fh);

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
    if (rc >= 0)
        return -EIO;
    /*
     * libnfs maps NFS4ERR_EXPIRED/GRACE/STALE_CLIENTID to ERANGE (-34).
     * ERANGE is not a meaningful I/O error for applications — they don't
     * know what "Numerical result out of range" means.  Map it to EIO.
     */
    if (rc == -ERANGE)
        return -EIO;
    return rc;
}

static int nfs_err_log(int rc, const char *op, const char *path,
                       struct nfs_context *ctx)
{
    log_nfs_error(op, path, rc, ctx);
    return nfs_err(rc);
}

static struct nfs_context *mount_new_context(const char *url);

/*
 * ======================================================================
 * Async NFS event loop infrastructure (--async mode)
 *
 * Replaces blocking libnfs sync calls with async request + poll loop.
 * Key advantage: poll() has a short timeout (5s), so meta_lock is
 * released during the wait and reacquired only briefly for
 * nfs_service().  A stuck NFS server causes poll() timeouts, not a
 * permanently held lock.
 * ======================================================================
 */

#define ASYNC_POLL_MS  5000  /* 5 second poll timeout */

struct async_result {
    int done;
    int err;     /* negative errno on error, or bytes for read/write */
    void *data;  /* operation-specific result (stat, fh, etc.) */
};

static void async_generic_cb(int err, struct nfs_context *nfs,
                              void *data, void *private_data)
{
    struct async_result *ar = (struct async_result *)private_data;
    (void)nfs;
    ar->err = err;
    ar->data = data;
    ar->done = 1;
}

/*
 * Run the NFS event loop until the async operation completes, or
 * stuck_timeout/dead_triggered fires.  meta_lock is released during
 * poll() so other threads (watchdog, keepalive) are never blocked.
 *
 * Returns 0 on success, or negative errno on error/timeout.
 * The caller must hold meta_lock on entry; it will hold it on return.
 */
static int async_event_loop(struct nfs_context *ctx, struct async_result *ar)
{
    time_t start = time(NULL);
    int timeout = g_state.stuck_timeout > 0 ? g_state.stuck_timeout : 120;

    while (!ar->done && !g_state.dead_triggered) {
        struct pollfd pfd;
        int r;

        pfd.fd = nfs_get_fd(ctx);
        pfd.events = nfs_which_events(ctx);
        pfd.revents = 0;

        if (pfd.fd < 0) {
            /* Context has no socket — connection lost */
            return -EIO;
        }

        /* Release lock during poll so watchdog/keepalive can run */
        pthread_mutex_unlock(&g_state.meta_lock);

        r = poll(&pfd, 1, ASYNC_POLL_MS);

        pthread_mutex_lock(&g_state.meta_lock);

        if (r > 0) {
            /* Socket has data/events — process them */
            if (nfs_service(ctx, pfd.revents) != 0) {
                DBG(1, "nfsfuse: async: nfs_service error\n");
                return -EIO;
            }
        } else if (r < 0 && errno != EINTR) {
            DBG(1, "nfsfuse: async: poll error: %s\n", strerror(errno));
            return -EIO;
        }

        /* Check timeout */
        if ((time(NULL) - start) >= timeout) {
            DBG(1, "nfsfuse: async: operation timed out after %ds\n",
                timeout);
            if (g_log_errors)
                syslog(LOG_WARNING,
                       "async NFS operation timed out after %d seconds",
                       timeout);
            return -ETIMEDOUT;
        }

        /* Update watchdog timestamp */
        if (g_state.has_dead_timeout)
            g_state.nfs_call_start = time(NULL);
    }

    if (g_state.dead_triggered)
        return -EIO;

    return 0;  /* ar->err has the NFS result */
}

/*
 * Async wrappers for key NFS operations.
 * Each acquires meta_lock, sends the async request, runs the event
 * loop, and returns with meta_lock released.
 */

static int async_nfs_lstat64(struct nfs_context *ctx, const char *path,
                              struct nfs_stat_64 *st)
{
    struct async_result ar = {0};
    int rc;

    pthread_mutex_lock(&g_state.meta_lock);
    rc = nfs_lstat64_async(ctx, path, async_generic_cb, &ar);
    if (rc != 0) {
        pthread_mutex_unlock(&g_state.meta_lock);
        return rc;
    }

    rc = async_event_loop(ctx, &ar);
    pthread_mutex_unlock(&g_state.meta_lock);

    if (rc != 0)
        return rc;
    if (ar.err < 0)
        return ar.err;
    if (ar.data && st)
        memcpy(st, ar.data, sizeof(*st));
    return 0;
}

static int async_nfs_open(struct nfs_context *ctx, const char *path,
                           int flags, struct nfsfh **fh_out)
{
    struct async_result ar = {0};
    int rc;

    pthread_mutex_lock(&g_state.meta_lock);
    rc = nfs_open_async(ctx, path, flags, async_generic_cb, &ar);
    if (rc != 0) {
        pthread_mutex_unlock(&g_state.meta_lock);
        return rc;
    }

    rc = async_event_loop(ctx, &ar);
    pthread_mutex_unlock(&g_state.meta_lock);

    if (rc != 0)
        return rc;
    if (ar.err < 0)
        return ar.err;
    if (fh_out)
        *fh_out = (struct nfsfh *)ar.data;
    return 0;
}

static int async_nfs_creat(struct nfs_context *ctx, const char *path,
                            int mode, struct nfsfh **fh_out)
{
    struct async_result ar = {0};
    int rc;

    pthread_mutex_lock(&g_state.meta_lock);
    rc = nfs_creat_async(ctx, path, mode, async_generic_cb, &ar);
    if (rc != 0) {
        pthread_mutex_unlock(&g_state.meta_lock);
        return rc;
    }

    rc = async_event_loop(ctx, &ar);
    pthread_mutex_unlock(&g_state.meta_lock);

    if (rc != 0)
        return rc;
    if (ar.err < 0)
        return ar.err;
    if (fh_out)
        *fh_out = (struct nfsfh *)ar.data;
    return 0;
}

static int async_nfs_close(struct nfs_context *ctx, struct nfsfh *fh)
{
    struct async_result ar = {0};
    int rc;

    pthread_mutex_lock(&g_state.meta_lock);
    rc = nfs_close_async(ctx, fh, async_generic_cb, &ar);
    if (rc != 0) {
        pthread_mutex_unlock(&g_state.meta_lock);
        return rc;
    }

    rc = async_event_loop(ctx, &ar);
    pthread_mutex_unlock(&g_state.meta_lock);

    if (rc != 0)
        return rc;
    return ar.err < 0 ? ar.err : 0;
}

static int async_nfs_pread(struct nfs_context *ctx, struct nfsfh *fh,
                            uint64_t offset, size_t count, char *buf)
{
    struct async_result ar = {0};
    int rc;

    pthread_mutex_lock(&g_state.meta_lock);
    rc = nfs_pread_async(ctx, fh, (void *)buf, (size_t)count,
                         (uint64_t)offset, async_generic_cb, &ar);
    if (rc != 0) {
        pthread_mutex_unlock(&g_state.meta_lock);
        return rc;
    }

    rc = async_event_loop(ctx, &ar);
    pthread_mutex_unlock(&g_state.meta_lock);

    if (rc != 0)
        return rc;
    if (ar.err < 0)
        return ar.err;
    return ar.err;  /* bytes read; data written directly to buf */
}

static int async_nfs_pwrite(struct nfs_context *ctx, struct nfsfh *fh,
                             uint64_t offset, size_t count, const char *buf)
{
    struct async_result ar = {0};
    int rc;

    pthread_mutex_lock(&g_state.meta_lock);
    rc = nfs_pwrite_async(ctx, fh, (const void *)buf, (size_t)count,
                          (uint64_t)offset, async_generic_cb, &ar);
    if (rc != 0) {
        pthread_mutex_unlock(&g_state.meta_lock);
        return rc;
    }

    rc = async_event_loop(ctx, &ar);
    pthread_mutex_unlock(&g_state.meta_lock);

    if (rc != 0)
        return rc;
    return ar.err;  /* bytes written, or negative errno */
}

static int async_nfs_fsync(struct nfs_context *ctx, struct nfsfh *fh)
{
    struct async_result ar = {0};
    int rc;

    pthread_mutex_lock(&g_state.meta_lock);
    rc = nfs_fsync_async(ctx, fh, async_generic_cb, &ar);
    if (rc != 0) {
        pthread_mutex_unlock(&g_state.meta_lock);
        return rc;
    }

    rc = async_event_loop(ctx, &ar);
    pthread_mutex_unlock(&g_state.meta_lock);

    if (rc != 0)
        return rc;
    return ar.err < 0 ? ar.err : 0;
}

static int async_nfs_fstat64(struct nfs_context *ctx, struct nfsfh *fh,
                              struct nfs_stat_64 *st)
{
    struct async_result ar = {0};
    int rc;

    pthread_mutex_lock(&g_state.meta_lock);
    rc = nfs_fstat64_async(ctx, fh, async_generic_cb, &ar);
    if (rc != 0) {
        pthread_mutex_unlock(&g_state.meta_lock);
        return rc;
    }

    rc = async_event_loop(ctx, &ar);
    pthread_mutex_unlock(&g_state.meta_lock);

    if (rc != 0)
        return rc;
    if (ar.err < 0)
        return ar.err;
    if (ar.data && st)
        memcpy(st, ar.data, sizeof(*st));
    return 0;
}

static int async_nfs_statvfs(struct nfs_context *ctx, const char *path,
                              struct statvfs *st)
{
    struct async_result ar = {0};
    int rc;

    pthread_mutex_lock(&g_state.meta_lock);
    rc = nfs_statvfs_async(ctx, path, async_generic_cb, &ar);
    if (rc != 0) {
        pthread_mutex_unlock(&g_state.meta_lock);
        return rc;
    }

    rc = async_event_loop(ctx, &ar);
    pthread_mutex_unlock(&g_state.meta_lock);

    if (rc != 0)
        return rc;
    if (ar.err < 0)
        return ar.err;
    if (ar.data && st)
        memcpy(st, ar.data, sizeof(*st));
    return 0;
}

/* --- Async META_RETRY: same retry logic but using async event loop --- */
#define ASYNC_META_RETRY(rc, op, path, async_call) do {                  \
    int _retries = 0;                                                    \
    int _did_reconnect = 0;                                              \
    for (;;) {                                                           \
        if (g_state.dead_triggered) { (rc) = -EIO; break; }              \
        (rc) = (async_call);                                             \
        if ((rc) >= 0) {                                                 \
            dead_timeout_on_success();                                    \
            if (_retries > 0 && g_log_errors)                            \
                syslog(LOG_NOTICE, "%s %s recovered after %d retries",   \
                       (op), (path) ? (path) : "", _retries);            \
            break;                                                       \
        }                                                                \
        dead_timeout_on_failure();                                       \
        if (g_state.dead_triggered) { (rc) = -EIO; break; }              \
        int _cls = classify_nfs_error(rc);                               \
        if (_cls == RETRY_NONE || _retries >= g_nfs4_retry_max)          \
            break;                                                       \
        _retries++;                                                      \
        if (_cls == RETRY_RECONNECT && !_did_reconnect) {                \
            if (reconnect_meta_context((rc), (op), (path)) == 0)         \
                _did_reconnect = 1;                                      \
            else                                                         \
                break;                                                   \
        } else {                                                         \
            DBG(1, "nfsfuse: %s on %s %s — waiting %ds (retry %d/%d)\n", \
                reconnect_reason(rc), (op), (path) ? (path) : "",        \
                g_nfs4_retry_wait, _retries, g_nfs4_retry_max);          \
            if (g_log_errors)                                            \
                syslog(LOG_WARNING, "%s on %s %s — retry %d/%d",         \
                       reconnect_reason(rc), (op),                       \
                       (path) ? (path) : "",                             \
                       _retries, g_nfs4_retry_max);                      \
            sleep(g_nfs4_retry_wait);                                    \
        }                                                                \
    }                                                                    \
} while (0)

/* --- End async infrastructure --- */

/* --- Deferred close function implementations --- */

static void deferred_close_add(const char *path, struct nfs_context *ctx,
                               struct nfsfh *fh)
{
    int i, oldest = -1;
    time_t oldest_time = 0;

    pthread_mutex_lock(&g_deferred_lock);

    for (i = 0; i < DEFERRED_CLOSE_MAX; i++) {
        if (!g_deferred[i].active) {
            oldest = i;
            break;
        }
        if (oldest < 0 || g_deferred[i].created < oldest_time) {
            oldest = i;
            oldest_time = g_deferred[i].created;
        }
    }

    if (g_deferred[oldest].active) {
        DBG(2, "nfsfuse: deferred close evict %s\n", g_deferred[oldest].path);
        if (pthread_mutex_trylock(&g_state.meta_lock) == 0) {
            nfs_close(g_deferred[oldest].ctx, g_deferred[oldest].fh);
            pthread_mutex_unlock(&g_state.meta_lock);
        } else {
            /* meta_lock busy — abandon the handle to avoid blocking
             * the FUSE thread.  Small resource leak but prevents
             * the mount from freezing during pool thrashing. */
            DBG(1, "nfsfuse: deferred close evict: meta_lock busy, "
                   "abandoning handle %s\n", g_deferred[oldest].path);
        }
    }

    snprintf(g_deferred[oldest].path, sizeof(g_deferred[oldest].path),
             "%s", path ? path : "");
    g_deferred[oldest].ctx = ctx;
    g_deferred[oldest].fh = fh;
    g_deferred[oldest].created = time(NULL);
    g_deferred[oldest].active = 1;

    DBG(2, "nfsfuse: deferred close hold %s (slot %d)\n",
        path ? path : "", oldest);

    pthread_mutex_unlock(&g_deferred_lock);
}

static void deferred_close_release(const char *path)
{
    int i;

    if (path == NULL)
        return;

    pthread_mutex_lock(&g_deferred_lock);
    for (i = 0; i < DEFERRED_CLOSE_MAX; i++) {
        if (g_deferred[i].active && strcmp(g_deferred[i].path, path) == 0) {
            DBG(2, "nfsfuse: deferred close release %s\n", path);
            pthread_mutex_lock(&g_state.meta_lock);
            nfs_close(g_deferred[i].ctx, g_deferred[i].fh);
            pthread_mutex_unlock(&g_state.meta_lock);
            g_deferred[i].active = 0;
        }
    }
    pthread_mutex_unlock(&g_deferred_lock);
}

static void deferred_close_expire(void)
{
    int i;
    time_t now = time(NULL);

    pthread_mutex_lock(&g_deferred_lock);
    for (i = 0; i < DEFERRED_CLOSE_MAX; i++) {
        if (g_deferred[i].active &&
            (now - g_deferred[i].created) >= DEFERRED_CLOSE_SEC) {
            DBG(2, "nfsfuse: deferred close expire %s\n", g_deferred[i].path);
            if (pthread_mutex_trylock(&g_state.meta_lock) == 0) {
                nfs_close(g_deferred[i].ctx, g_deferred[i].fh);
                pthread_mutex_unlock(&g_state.meta_lock);
                g_deferred[i].active = 0;
            } else {
                DBG(3, "nfsfuse: deferred close: meta_lock busy, "
                       "skipping %s\n", g_deferred[i].path);
            }
        }
    }
    pthread_mutex_unlock(&g_deferred_lock);
}

static void deferred_close_all(void)
{
    int i;

    pthread_mutex_lock(&g_deferred_lock);
    for (i = 0; i < DEFERRED_CLOSE_MAX; i++) {
        if (g_deferred[i].active) {
            nfs_close(g_deferred[i].ctx, g_deferred[i].fh);
            g_deferred[i].active = 0;
        }
    }
    pthread_mutex_unlock(&g_deferred_lock);
}

static int deferred_close_fstat(const char *path, struct nfs_stat_64 *st)
{
    int i, found = 0;

    if (path == NULL)
        return 0;

    pthread_mutex_lock(&g_deferred_lock);
    for (i = 0; i < DEFERRED_CLOSE_MAX; i++) {
        if (g_deferred[i].active && strcmp(g_deferred[i].path, path) == 0) {
            pthread_mutex_lock(&g_state.meta_lock);
            if (nfs_fstat64(g_deferred[i].ctx, g_deferred[i].fh, st) == 0)
                found = 1;
            pthread_mutex_unlock(&g_state.meta_lock);
            break;
        }
    }
    pthread_mutex_unlock(&g_deferred_lock);

    return found;
}

/*
 * Peek at a deferred handle — return the ctx/fh for use by the caller
 * WITHOUT removing from the pool.  The entry stays active so that
 * subsequent getattr calls can still recover via deferred_close_fstat.
 * The caller must NOT close the returned handle — it is still owned
 * by the pool and will be closed by release/expire/shutdown.
 */
static int deferred_close_peek(const char *path, struct nfs_context **out_ctx,
                               struct nfsfh **out_fh)
{
    int i, found = 0;

    if (path == NULL)
        return 0;

    pthread_mutex_lock(&g_deferred_lock);
    for (i = 0; i < DEFERRED_CLOSE_MAX; i++) {
        if (g_deferred[i].active && strcmp(g_deferred[i].path, path) == 0) {
            *out_ctx = g_deferred[i].ctx;
            *out_fh = g_deferred[i].fh;
            found = 1;
            DBG(2, "nfsfuse: deferred close peek %s\n", path);
            break;
        }
    }
    pthread_mutex_unlock(&g_deferred_lock);

    return found;
}

/* --- End deferred close implementations --- */

/*
 * Classify NFS errors for retry strategy:
 *   RETRY_RECONNECT — session expired, need to remount (EXPIRED, STALE)
 *   RETRY_WAIT      — server temporarily unavailable (GRACE period)
 *   RETRY_NONE      — permanent error, don't retry
 *
 * libnfs maps NFS4ERR_EXPIRED, NFS4ERR_GRACE, and NFS4ERR_STALE_CLIENTID
 * all to ERANGE (-34).  We cannot distinguish them by errno alone, so we
 * treat all ERANGE as retriable.  First attempt reconnects (covers
 * EXPIRED/STALE_CLIENTID), subsequent attempts wait (covers GRACE).
 */
#define RETRY_NONE       0
#define RETRY_RECONNECT  1
#define RETRY_WAIT       2

static int g_nfs4_retry_max = 5;
static int g_nfs4_retry_wait = 30;

/*
 * Dead-server watchdog: track consecutive failures.
 * Call dead_timeout_on_failure() when an NFS operation fails.
 * Call dead_timeout_on_success() when an NFS operation succeeds.
 * If failures persist for dead_timeout seconds, trigger FUSE unmount.
 */
static struct fuse *g_fuse_instance;  /* set in nfuse_init */

static long long now_ms(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static void dead_timeout_on_failure(void)
{
    long long now;

    if (g_state.dead_timeout <= 0 || g_state.dead_triggered)
        return;

    now = now_ms();

    if (g_state.first_failure_ms == 0) {
        g_state.first_failure_ms = now;
        DBG(1, "nfsfuse: dead-timeout watchdog started (limit %ds)\n",
            g_state.dead_timeout);
        if (g_log_errors)
            syslog(LOG_WARNING,
                   "connectivity lost — will unmount after %d seconds",
                   g_state.dead_timeout);
        return;
    }

    if ((now - g_state.first_failure_ms) >= (long long)g_state.dead_timeout * 1000) {
        g_state.dead_triggered = 1;
        long long elapsed_ms = now - g_state.first_failure_ms;
        DBG(1, "nfsfuse: dead-timeout reached (%lld.%03llds) — triggering unmount\n",
            elapsed_ms / 1000, elapsed_ms % 1000);
        if (g_log_errors)
            syslog(LOG_CRIT,
                   "server unreachable for %lld.%03lld seconds — unmounting",
                   elapsed_ms / 1000, elapsed_ms % 1000);
        if (g_fuse_instance)
            fuse_exit(g_fuse_instance);
    }
}

static void dead_timeout_on_success(void)
{
    if (g_state.dead_timeout <= 0)
        return;

    g_state.last_nfs_success = time(NULL);

    if (g_state.first_failure_ms != 0) {
        long long elapsed_ms = now_ms() - g_state.first_failure_ms;
        DBG(1, "nfsfuse: dead-timeout watchdog reset — server responded "
               "after %lld.%03llds\n", elapsed_ms / 1000, elapsed_ms % 1000);
        if (g_log_errors)
            syslog(LOG_NOTICE, "connectivity restored after %lld.%03lld seconds",
                   elapsed_ms / 1000, elapsed_ms % 1000);
    }
    g_state.first_failure_ms = 0;
}

static int classify_nfs_error(int rc)
{
    if (g_state.safe_v4_mode && rc == -ERANGE)
        return RETRY_RECONNECT;
    if (g_state.safe_v4_mode && rc == -EINVAL)
        return RETRY_RECONNECT;  /* NFS4ERR_BADHANDLE after reconnect */
    if (rc == -ETIMEDOUT)
        return RETRY_WAIT;
    if (g_reconnect_on_stale && rc == -ESTALE)
        return RETRY_RECONNECT;
    if (g_reconnect_on_io_error && rc == -EIO)
        return RETRY_RECONNECT;
    return RETRY_NONE;
}

static const char *reconnect_reason(int rc)
{
    if (rc == -ERANGE)
        return "NFS4ERR_EXPIRED/GRACE";
    if (rc == -EINVAL)
        return "NFS4ERR_BADHANDLE";
    if (rc == -EIO)
        return "I/O error";
    if (rc == -ESTALE)
        return "ESTALE";
    if (rc == -ETIMEDOUT)
        return "timeout";
    return "NFS error";
}

static int reconnect_meta_context(int rc, const char *op, const char *path)
{
    struct nfs_context *new_ctx;
    const char *reason = reconnect_reason(rc);

    DBG(1, "nfsfuse: %s on %s %s — reconnecting\n",
        reason, op, path ? path : "");

    if (g_log_errors)
        syslog(LOG_WARNING, "%s on %s %s — reconnecting",
               reason, op, path ? path : "");

    new_ctx = mount_new_context(g_state.url_effective);
    if (new_ctx == NULL) {
        DBG(1, "nfsfuse: reconnect failed\n");
        if (g_log_errors)
            syslog(LOG_ERR, "reconnect failed after %s on %s %s",
                   reason, op, path ? path : "");
        return -1;
    }

    /*
     * Flush the deferred close pool BEFORE swapping contexts.
     * Deferred entries hold ctx/fh pointers from the old session.
     * After reconnect, those pointers reference a dead NFS session
     * and using them causes segfaults (use-after-free in libnfs).
     * We can't properly close them (old session is dead), so just
     * mark them inactive and abandon the handles.
     */
    {
        int _i;
        pthread_mutex_lock(&g_deferred_lock);
        for (_i = 0; _i < DEFERRED_CLOSE_MAX; _i++) {
            if (g_deferred[_i].active) {
                DBG(1, "nfsfuse: reconnect: discarding deferred handle %s\n",
                    g_deferred[_i].path);
                /* Don't nfs_close — old context is dead, would crash */
                g_deferred[_i].active = 0;
            }
        }
        pthread_mutex_unlock(&g_deferred_lock);
    }

    pthread_mutex_lock(&g_state.meta_lock);
    {
        /*
         * Neutralize the old context before swapping.  We can't destroy
         * it (open file handles reference its internal nfsfh structs),
         * but we MUST stop it from doing anything:
         *
         * 1. Disable autoreconnect — prevents libnfs from trying to
         *    reconnect the dead TCP connection in the background, which
         *    would allocate/free internal state and cause use-after-free.
         *
         * 2. Shut down the socket — ensures no background I/O, no
         *    callbacks, no internal state changes.  Any pending poll()
         *    returns immediately.
         *
         * The old context becomes an inert shell: its nfsfh structs
         * are still valid memory but unusable.  file_handle_ctx()
         * returns g_state.meta_nfs (the new context), so no code path
         * will call libnfs functions on the old context.
         */
        struct nfs_context *old_ctx = g_state.meta_nfs;
        if (old_ctx) {
            int old_fd = nfs_get_fd(old_ctx);
            nfs_set_autoreconnect(old_ctx, 0);
            if (old_fd >= 0)
                shutdown(old_fd, SHUT_RDWR);
        }
    }
    g_state.meta_nfs = new_ctx;
    pthread_mutex_unlock(&g_state.meta_lock);

    DBG(1, "nfsfuse: reconnected successfully\n");
    if (g_log_errors)
        syslog(LOG_NOTICE, "reconnected after %s, retrying %s %s",
               reason, op, path ? path : "");

    return 0;
}

/*
 * Resilient metadata operation wrapper.  Handles transient NFS4 errors:
 *
 *   1. On EXPIRED/STALE_CLIENTID: reconnect (remount) and retry.
 *   2. On GRACE/timeout: wait g_nfs4_retry_wait seconds and retry
 *      without reconnecting — the server is recovering and the current
 *      session will become valid again once the grace period ends.
 *   3. Retry up to g_nfs4_retry_max times with increasing wait, so we
 *      can survive a ~30-second grace period.
 *
 * The 'call' expression must use g_state.meta_nfs (updated on reconnect).
 */
/*
 * META_RETRY_ASYNC: version that accepts both sync and async call exprs.
 * In async mode, uses the async_call which manages meta_lock internally
 * (acquires briefly for nfs_service, releases during poll).
 * In sync mode, uses the blocking call with meta_lock held throughout.
 */
#define META_RETRY_ASYNC(rc, op, path, sync_call, async_call) do {       \
    int _retries = 0;                                                    \
    int _did_reconnect = 0;                                              \
    for (;;) {                                                           \
        if (g_state.dead_triggered) { (rc) = -EIO; break; }              \
        if (g_async_mode) {                                              \
            if (g_state.has_dead_timeout)                                 \
                g_state.nfs_call_start = time(NULL);                     \
            (rc) = (async_call);                                         \
            if (g_state.has_dead_timeout)                                 \
                g_state.nfs_call_start = 0;                              \
        } else {                                                         \
            pthread_mutex_lock(&g_state.meta_lock);                      \
            if (g_state.has_dead_timeout)                                 \
                g_state.nfs_call_start = time(NULL);                     \
            (rc) = (sync_call);                                          \
            if (g_state.has_dead_timeout)                                 \
                g_state.nfs_call_start = 0;                              \
            pthread_mutex_unlock(&g_state.meta_lock);                    \
        }                                                                \
        if ((rc) >= 0) {                                                 \
            dead_timeout_on_success();                                    \
            if (_retries > 0 && g_log_errors)                            \
                syslog(LOG_NOTICE, "%s %s recovered after %d retries",   \
                       (op), (path) ? (path) : "", _retries);            \
            break;                                                       \
        }                                                                \
        dead_timeout_on_failure();                                       \
        if (g_state.dead_triggered) { (rc) = -EIO; break; }              \
        int _cls = classify_nfs_error(rc);                               \
        if (_cls == RETRY_NONE || _retries >= g_nfs4_retry_max)          \
            break;                                                       \
        _retries++;                                                      \
        if (_cls == RETRY_RECONNECT && !_did_reconnect) {                \
            if (reconnect_meta_context((rc), (op), (path)) == 0)         \
                _did_reconnect = 1;                                      \
            else                                                         \
                break;                                                   \
        } else {                                                         \
            DBG(1, "nfsfuse: %s on %s %s — waiting %ds (retry %d/%d)\n", \
                reconnect_reason(rc), (op), (path) ? (path) : "",        \
                g_nfs4_retry_wait, _retries, g_nfs4_retry_max);          \
            if (g_log_errors)                                            \
                syslog(LOG_WARNING, "%s on %s %s — retry %d/%d",         \
                       reconnect_reason(rc), (op),                       \
                       (path) ? (path) : "",                             \
                       _retries, g_nfs4_retry_max);                      \
            sleep(g_nfs4_retry_wait);                                    \
        }                                                                \
    }                                                                    \
} while (0)

/* Backward-compatible: sync-only META_RETRY */
#define META_RETRY(rc, op, path, call) do {                              \
    int _retries = 0;                                                    \
    int _did_reconnect = 0;                                              \
    for (;;) {                                                           \
        if (g_state.dead_triggered) { (rc) = -EIO; break; }              \
        pthread_mutex_lock(&g_state.meta_lock);                          \
        if (g_state.has_dead_timeout) g_state.nfs_call_start = time(NULL); \
        (rc) = (call);                                                   \
        if (g_state.has_dead_timeout) g_state.nfs_call_start = 0;        \
        pthread_mutex_unlock(&g_state.meta_lock);                        \
        if ((rc) >= 0) {                                                 \
            dead_timeout_on_success();                                    \
            if (_retries > 0 && g_log_errors)                            \
                syslog(LOG_NOTICE, "%s %s recovered after %d retries",   \
                       (op), (path) ? (path) : "", _retries);            \
            break;                                                       \
        }                                                                \
        dead_timeout_on_failure();                                       \
        if (g_state.dead_triggered) { (rc) = -EIO; break; }              \
        int _cls = classify_nfs_error(rc);                               \
        if (_cls == RETRY_NONE || _retries >= g_nfs4_retry_max)            \
            break;                                                       \
        _retries++;                                                      \
        if (_cls == RETRY_RECONNECT && !_did_reconnect) {                \
            if (reconnect_meta_context((rc), (op), (path)) == 0)         \
                _did_reconnect = 1;                                      \
            else                                                         \
                break;                                                   \
        } else {                                                         \
            DBG(1, "nfsfuse: %s on %s %s — waiting %ds (retry %d/%d)\n", \
                reconnect_reason(rc), (op), (path) ? (path) : "",        \
                g_nfs4_retry_wait, _retries, g_nfs4_retry_max);          \
            if (g_log_errors)                                            \
                syslog(LOG_WARNING, "%s on %s %s — retry %d/%d",         \
                       reconnect_reason(rc), (op),                       \
                       (path) ? (path) : "",                             \
                       _retries, g_nfs4_retry_max);                        \
            sleep(g_nfs4_retry_wait);                                  \
        }                                                                \
    }                                                                    \
} while (0)

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

static void apply_nfs_tuning(struct nfs_context *ctx)
{
    if (g_state.has_timeout) {
        nfs_set_timeout(ctx, g_state.timeout);
        DBG(1, "  timeout=%dms\n", g_state.timeout);
    }
    if (g_state.has_retrans) {
        nfs_set_retrans(ctx, g_state.retrans);
        DBG(1, "  retrans=%d\n", g_state.retrans);
    }
    if (g_state.has_autoreconnect) {
        /*
         * For NFS4, TCP-level autoreconnect is dangerous: it silently
         * creates a new NFS4 session (new clientid), invalidating all
         * stateids from the old session.  Open file handles still
         * reference the old context and will get errors.  Limit
         * autoreconnect to at most 3 attempts for NFS4 to allow
         * transient recovery without endless silent state loss.
         */
        int ar = g_state.autoreconnect;
        if (g_state.safe_v4_mode && ar > 3)
            ar = 3;
        nfs_set_autoreconnect(ctx, ar);
        DBG(1, "  autoreconnect=%d%s\n", ar,
            (ar != g_state.autoreconnect) ? " (capped for NFS4)" : "");
    }
    if (g_state.has_tcp_syncnt) {
        nfs_set_tcp_syncnt(ctx, g_state.tcp_syncnt);
        DBG(1, "  tcp_syncnt=%d\n", g_state.tcp_syncnt);
    }
    if (g_state.has_poll_timeout) {
        nfs_set_poll_timeout(ctx, g_state.poll_timeout);
        DBG(1, "  poll_timeout=%dms\n", g_state.poll_timeout);
    }
}

static struct nfs_context *mount_new_context(const char *url)
{
    struct nfs_context *ctx = NULL;
    struct nfs_url *nurl = NULL;

    DBG(1, "  nfs_init_context...\n");
    ctx = nfs_init_context();
    if (ctx == NULL)
        return NULL;

    apply_nfs_tuning(ctx);

    DBG(1, "  nfs_parse_url_dir...\n");
    nurl = nfs_parse_url_dir(ctx, url);
    if (nurl == NULL) {
        fprintf(stderr, "nfsfuse: url parse failed: %s\n", nfs_get_error(ctx));
        nfs_destroy_context(ctx);
        return NULL;
    }

    DBG(1, "  server=%s path=%s\n",
        nurl->server ? nurl->server : "(null)",
        nurl->path ? nurl->path : "(null)");

    DBG(1, "  nfs_mount...\n");
    if (nfs_mount(ctx, nurl->server, nurl->path) != 0) {
        fprintf(stderr, "nfsfuse: mount failed: %s\n", nfs_get_error(ctx));
        nfs_destroy_url(nurl);
        nfs_destroy_context(ctx);
        return NULL;
    }

    DBG(1, "  mount ok\n");
    nfs_destroy_url(nurl);
    return ctx;
}

/*
 * NFS4 lease keepalive thread.  NFS4 requires the client to renew its
 * lease periodically (typically every 90 seconds).  In single-threaded
 * FUSE mode, long-running I/O (e.g. 32 GB VMDK reads) can block the
 * FUSE thread for minutes, preventing any NFS4 lease renewal from
 * happening through normal traffic.  This background thread sends a
 * lightweight getattr("/") every 30 seconds to keep the lease alive.
 */
static void *keepalive_thread_func(void *arg)
{
    struct nfs_stat_64 st;

    (void)arg;

    while (g_state.keepalive_running) {
        /*
         * Use short sleeps so the thread can exit promptly on unmount.
         * Normal cycle: ~30 seconds (60 * 0.5s).
         * When dead-timeout failure tracking is active: ~5 seconds
         * (10 * 0.5s) so the watchdog can advance the counter faster.
         */
        int cycles = (g_state.meta_lock_busy_since != 0 ||
                      (g_state.has_dead_timeout && g_state.first_failure_ms != 0))
                     ? 10 : 60;
        int i;
        for (i = 0; i < cycles && g_state.keepalive_running; i++)
            usleep(500000);

        if (!g_state.keepalive_running)
            break;

        /* Log dead-timeout watchdog status at level 3 */
        if (g_state.has_dead_timeout) {
            long long elapsed_ms = g_state.first_failure_ms
                                   ? (now_ms() - g_state.first_failure_ms) : 0;
            DBG(3, "nfsfuse: watchdog tick: dead_timeout=%ds "
                   "elapsed=%lld.%03llds dead_triggered=%d "
                   "cycle=%ds\n",
                g_state.dead_timeout,
                elapsed_ms / 1000, elapsed_ms % 1000,
                g_state.dead_triggered, cycles / 2);
        }

        /*
         * Try to acquire the meta lock without blocking.  If the lock
         * is held (e.g. a write is stuck in a blocking NFS call), we
         * still need to advance the dead-timeout counter — otherwise
         * the watchdog is completely blocked and can never trigger the
         * unmount.
         *
         * When trylock succeeds, nfs_lstat64() may still block for up
         * to timeout*retrans ms on a dead server, but it will
         * eventually return an error and dead_timeout_on_failure()
         * will be called.
         */
        if (pthread_mutex_trylock(&g_state.meta_lock) == 0) {
            if (g_state.meta_lock_busy_since != 0) {
                /* Lock was busy, now free — stuck-call recovery worked */
                DBG(1, "nfsfuse: stuck-call recovery: meta_lock released, "
                       "mount recovered\n");
                if (g_log_errors)
                    syslog(LOG_NOTICE,
                           "stuck-call recovery: NFS call unblocked, "
                           "mount recovered");
            }
            g_state.meta_lock_busy_since = 0;
            if (g_state.meta_nfs) {
                DBG(3, "nfsfuse: watchdog: calling nfs_lstat64...\n");
                int ka_rc = nfs_lstat64(g_state.meta_nfs, "/", &st);
                if (ka_rc < 0) {
                    dead_timeout_on_failure();
                    DBG(1, "nfsfuse: keepalive failed (rc=%d)\n", ka_rc);
                } else {
                    dead_timeout_on_success();
                    DBG(3, "nfsfuse: watchdog: keepalive OK\n");
                }
            }
            pthread_mutex_unlock(&g_state.meta_lock);
        } else {
            /* Lock held — another thread is stuck in an NFS call */
            DBG(1, "nfsfuse: watchdog: meta_lock busy, "
                   "NFS call likely stuck — treating as failure\n");
            dead_timeout_on_failure();

            /*
             * Stuck-call recovery: if meta_lock has been held for
             * stuck_timeout seconds, shut down the NFS socket to
             * unblock the stuck call.  The call returns error,
             * releases meta_lock, and the retry logic reconnects.
             * The mount stays alive — no _exit(), no fuse_exit().
             */
            if (g_state.stuck_timeout > 0) {
                time_t now = time(NULL);
                if (g_state.meta_lock_busy_since == 0)
                    g_state.meta_lock_busy_since = now;

                long stuck_secs = (long)(now - g_state.meta_lock_busy_since);
                DBG(3, "nfsfuse: watchdog: meta_lock busy for %lds "
                       "(stuck_timeout=%ds)\n",
                    stuck_secs, g_state.stuck_timeout);

                if (stuck_secs >= g_state.stuck_timeout && g_state.meta_nfs) {
                    int nfs_fd = nfs_get_fd(g_state.meta_nfs);
                    DBG(1, "nfsfuse: stuck-call recovery: killing NFS "
                           "socket (fd=%d) after %lds\n",
                        nfs_fd, stuck_secs);
                    if (g_log_errors)
                        syslog(LOG_WARNING,
                               "stuck NFS call for %ld seconds "
                               "— killing socket to recover",
                               stuck_secs);
                    /*
                     * Disable autoreconnect FIRST so libnfs doesn't
                     * create a new socket after we kill this one.
                     * Then shutdown + close the fd to unblock any
                     * blocking syscall (poll, recv, send, connect).
                     */
                    nfs_set_autoreconnect(g_state.meta_nfs, 0);
                    if (nfs_fd >= 0) {
                        shutdown(nfs_fd, SHUT_RDWR);
                        close(nfs_fd);
                    }
                    g_state.meta_lock_busy_since = 0;
                }
            }
        }

        if (g_state.dead_triggered)
            break;

        deferred_close_expire();

        DBG(3, "nfsfuse: watchdog: cycle complete\n");
    }

    DBG(1, "nfsfuse: keepalive thread exiting (dead_triggered=%d)\n",
        g_state.dead_triggered);
    return NULL;
}

static int start_keepalive_thread(void)
{
    g_state.keepalive_running = 1;
    if (pthread_create(&g_state.keepalive_thread, NULL,
                       keepalive_thread_func, NULL) != 0) {
        g_state.keepalive_running = 0;
        return -1;
    }
    return 0;
}

static void stop_keepalive_thread(void)
{
    if (!g_state.keepalive_running)
        return;

    g_state.keepalive_running = 0;

    /*
     * Don't pthread_join — the thread may be blocked in nfs_lstat64()
     * for up to 60 seconds (NFS timeout).  Detach it so the process
     * can exit immediately.  The thread will be killed when the process
     * exits.
     */
    if (g_state.keepalive_thread) {
        pthread_detach(g_state.keepalive_thread);
        g_state.keepalive_thread = 0;
    }
}

/*
 * Dedicated dead-timeout watchdog thread.
 *
 * This thread does NO NFS calls and acquires NO mutexes.  It reads
 * only volatile timestamps set by the I/O threads and calls fuse_exit()
 * when the server has been unresponsive for dead_timeout seconds.
 *
 * This is necessary because the keepalive thread can itself be blocked:
 * - On meta_lock (held by a stuck write/read)
 * - On nfs_lstat64() (TCP socket stuck on dead server)
 * - On deferred_close_expire() (same lock issue)
 *
 * The watchdog detects two conditions:
 * 1. An NFS call has been in-flight for >= dead_timeout seconds
 *    (nfs_call_start != 0 and too old)
 * 2. No successful NFS operation for >= dead_timeout seconds
 *    (last_nfs_success != 0 and too old)
 */
static void *dead_timeout_watchdog_func(void *arg)
{
    (void)arg;

    DBG(1, "nfsfuse: dead-timeout watchdog thread started "
           "(timeout=%ds, checking every 2s)\n", g_state.dead_timeout);

    while (g_state.watchdog_running && !g_state.dead_triggered) {
        /* Sleep 2 seconds in short increments for prompt exit */
        int i;
        for (i = 0; i < 4 && g_state.watchdog_running; i++)
            usleep(500000);

        if (!g_state.watchdog_running)
            break;

        time_t now = time(NULL);
        time_t call_start = g_state.nfs_call_start;
        time_t last_success = g_state.last_nfs_success;

        long call_age = (call_start != 0) ? (long)(now - call_start) : 0;
        long since_success = (last_success != 0) ? (long)(now - last_success) : 0;

        DBG(3, "nfsfuse: dead-watchdog: call_in_flight=%s call_age=%lds "
               "since_last_success=%lds dead_triggered=%d\n",
            call_start ? "yes" : "no", call_age,
            since_success, g_state.dead_triggered);

        /* Condition 1: NFS call stuck for >= dead_timeout */
        if (call_start != 0 && call_age >= g_state.dead_timeout) {
            g_state.dead_triggered = 1;
            DBG(1, "nfsfuse: dead-watchdog: NFS call stuck for %lds "
                   "(limit %ds) — triggering unmount\n",
                call_age, g_state.dead_timeout);
            if (g_log_errors)
                syslog(LOG_CRIT,
                       "NFS call stuck for %ld seconds — unmounting",
                       call_age);
            if (g_fuse_instance)
                fuse_exit(g_fuse_instance);
            break;
        }

        /* Condition 2: no successful NFS op for >= dead_timeout */
        if (last_success != 0 && since_success >= g_state.dead_timeout) {
            g_state.dead_triggered = 1;
            DBG(1, "nfsfuse: dead-watchdog: no NFS success for %lds "
                   "(limit %ds) — triggering unmount\n",
                since_success, g_state.dead_timeout);
            if (g_log_errors)
                syslog(LOG_CRIT,
                       "no successful NFS operation for %ld seconds "
                       "— unmounting", since_success);
            if (g_fuse_instance)
                fuse_exit(g_fuse_instance);
            break;
        }
    }

    /*
     * Shut down the NFS TCP socket to unblock the stuck libnfs call.
     * shutdown() on a socket causes any blocking recv()/send()/poll()
     * on that socket to return immediately with an error.  This is
     * safe to call from any thread without holding meta_lock.
     *
     * After the stuck call returns an error, the FUSE I/O path sees
     * dead_triggered=1 and returns -EIO through the normal FUSE
     * response path.  The kernel delivers -EIO to the application
     * (e.g. QEMU), which handles EIO much better than ENOTCONN
     * (which is what happens if the FUSE daemon just _exit()s).
     */
    if (g_state.meta_nfs) {
        int nfs_fd = nfs_get_fd(g_state.meta_nfs);
        if (nfs_fd >= 0) {
            DBG(1, "nfsfuse: dead-watchdog: shutting down NFS socket "
                   "(fd=%d) to unblock stuck call\n", nfs_fd);
            shutdown(nfs_fd, SHUT_RDWR);
        }
    }

    if (g_log_errors)
        syslog(LOG_CRIT, "dead-timeout: NFS socket shut down, "
                          "returning EIO to applications");

    /*
     * Wait for the FUSE thread to process the error and exit the
     * event loop.  If it doesn't exit within 10 seconds (e.g. the
     * socket shutdown didn't unblock the call), force-exit as a
     * safety net.
     */
    DBG(1, "nfsfuse: dead-watchdog: waiting up to 10s for "
           "graceful shutdown\n");
    {
        int i;
        for (i = 0; i < 20; i++) {
            usleep(500000);
            if (!g_state.keepalive_running)
                break;  /* nfuse_destroy ran — clean exit happening */
        }
    }

    if (g_auto_remount && g_saved_argv) {
        DBG(1, "nfsfuse: dead-watchdog: auto-remount — re-execing\n");
        if (g_log_errors)
            syslog(LOG_CRIT, "dead-timeout: auto-remount — restarting");
        /* Re-exec ourselves with the same arguments.
         * execv replaces the process image, so the stale FUSE mount
         * is cleaned up by the kernel, and a fresh mount is created. */
        execv(g_saved_argv[0], g_saved_argv);
        /* execv only returns on error */
        DBG(1, "nfsfuse: dead-watchdog: execv failed: %s\n",
            strerror(errno));
    }

    DBG(1, "nfsfuse: dead-watchdog: force exit (safety net)\n");
    if (g_log_errors)
        syslog(LOG_CRIT, "dead-timeout: force-exiting");
    _exit(1);

    /* not reached */
    return NULL;
}

static int start_dead_timeout_watchdog(void)
{
    g_state.watchdog_running = 1;
    if (pthread_create(&g_state.watchdog_thread, NULL,
                       dead_timeout_watchdog_func, NULL) != 0) {
        g_state.watchdog_running = 0;
        return -1;
    }
    return 0;
}

static void stop_dead_timeout_watchdog(void)
{
    if (!g_state.watchdog_running)
        return;
    g_state.watchdog_running = 0;
    /* Detach — thread uses only volatile reads and will exit promptly */
    if (g_state.watchdog_thread) {
        pthread_detach(g_state.watchdog_thread);
        g_state.watchdog_thread = 0;
    }
}

static void cleanup_app_state(void)
{
    stop_dead_timeout_watchdog();
    stop_keepalive_thread();
    deferred_close_all();

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

/*
 * For shared-context handles (own_ctx=0), always use the current
 * g_state.meta_nfs rather than the stale h->ctx pointer.  After a
 * reconnect, g_state.meta_nfs points to the new context while h->ctx
 * still points to the old (dead) one.
 */
static struct nfs_context *file_handle_ctx(struct file_handle *h)
{
    return h->own_ctx ? h->ctx : g_state.meta_nfs;
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
        /*
         * Upgrade O_WRONLY to O_RDWR so the kernel page cache can
         * read back data for the same file handle (needed for
         * auto_cache).  Do NOT strip O_APPEND — removing it breaks
         * atomic append semantics and can cause data corruption when
         * multiple processes append to the same file.
         */
        if ((out & O_ACCMODE) == O_WRONLY) {
            out &= ~O_ACCMODE;
            out |= O_RDWR;
        }
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
        if (create)
            rc = nfs_creat(ctx, path, mode, &fh);
        else
            rc = nfs_open(ctx, path, flags, &fh);
    } else {
        pthread_mutex_lock(&g_state.meta_lock);
        if (create)
            rc = nfs_creat(ctx, path, mode, &fh);
        else
            rc = nfs_open(ctx, path, flags, &fh);
        pthread_mutex_unlock(&g_state.meta_lock);
    }

    if (rc < 0) {
        log_nfs_error("open", path, rc, ctx);
        if (use_private_ctx)
            destroy_nfs_context_safe(ctx);
        return nfs_err(rc);
    }

    /*
     * libnfs nfs_creat() on NFS4 may not properly set the file mode
     * in the OPEN+CREATE compound attrs, resulting in mode=0000 on the
     * server.  Explicitly chmod after create to ensure correct perms.
     */
    if (create && mode != 0) {
        if (use_private_ctx)
            nfs_fchmod(ctx, fh, mode);
        else {
            pthread_mutex_lock(&g_state.meta_lock);
            nfs_fchmod(ctx, fh, mode);
            pthread_mutex_unlock(&g_state.meta_lock);
        }
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
    h->writable = create || (flags & (O_WRONLY | O_RDWR));

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
        int io_retries = 0;

        if (g_state.max_mode && !g_state.safe_v4_mode && chunk > g_state.io_chunk)
            chunk = g_state.io_chunk;

        for (;;) {
            if (g_state.dead_triggered) { rc = -EIO; break; }
            if (g_state.has_dead_timeout) g_state.nfs_call_start = time(NULL);
            if (g_async_mode && !h->own_ctx) {
                file_handle_unlock(h);
                rc = async_nfs_pread(file_handle_ctx(h), h->fh,
                                     (uint64_t)(offset + (off_t)done),
                                     chunk, buf + done);
                file_handle_lock(h);
            } else {
                rc = CALL_NFS_PREAD(file_handle_ctx(h), h->fh, offset + (off_t)done, chunk, buf + done);
            }
            if (g_state.has_dead_timeout) g_state.nfs_call_start = 0;
            if (rc >= 0) {
                dead_timeout_on_success();
                if (io_retries > 0 && g_log_errors)
                    syslog(LOG_NOTICE,
                           "read recovered after %d retries", io_retries);
                break;
            }
            dead_timeout_on_failure();
            if (g_state.dead_triggered) { rc = -EIO; break; }
            int _rcls = classify_nfs_error(rc);
            if (_rcls == RETRY_NONE)
                break;
            if (io_retries >= g_nfs4_retry_max) {
                if (_rcls == RETRY_RECONNECT) {
                    file_handle_unlock(h);
                    reconnect_meta_context(rc, "read", NULL);
                    file_handle_lock(h);
                    rc = -EIO;
                }
                if (g_log_errors)
                    syslog(LOG_ERR,
                           "read failed after %d retries (rc=%d/%s)",
                           io_retries, rc, strerror(-rc));
                break;
            }
            io_retries++;
            DBG(1, "nfsfuse: read transient error — waiting %ds (retry %d/%d)\n",
                g_nfs4_retry_wait, io_retries, g_nfs4_retry_max);
            if (g_log_errors)
                syslog(LOG_WARNING, "read error — retry %d/%d (%s)",
                       io_retries, g_nfs4_retry_max,
                       reconnect_reason(rc));
            file_handle_unlock(h);
            sleep(g_nfs4_retry_wait);
            file_handle_lock(h);
        }

        if (rc < 0) {
            if (done == 0) {
                log_nfs_error("read", NULL, rc, file_handle_ctx(h));
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
        int io_retries = 0;

        if (g_state.max_mode && !g_state.safe_v4_mode && chunk > g_state.io_chunk)
            chunk = g_state.io_chunk;

        for (;;) {
            if (g_state.dead_triggered) { rc = -EIO; break; }
            if (g_state.has_dead_timeout) g_state.nfs_call_start = time(NULL);
            if (g_async_mode && !h->own_ctx) {
                file_handle_unlock(h);
                rc = async_nfs_pwrite(file_handle_ctx(h), h->fh,
                                      (uint64_t)(offset + (off_t)done),
                                      chunk, buf + done);
                file_handle_lock(h);
            } else {
                rc = CALL_NFS_PWRITE(file_handle_ctx(h), h->fh, offset + (off_t)done, chunk, buf + done);
            }
            if (g_state.has_dead_timeout) g_state.nfs_call_start = 0;
            if (rc >= 0) {
                dead_timeout_on_success();
                if (io_retries > 0 && g_log_errors)
                    syslog(LOG_NOTICE,
                           "write recovered after %d retries", io_retries);
                break;
            }
            dead_timeout_on_failure();
            if (g_state.dead_triggered) { rc = -EIO; break; }
            int _wcls = classify_nfs_error(rc);
            if (_wcls == RETRY_NONE)
                break;
            if (io_retries >= g_nfs4_retry_max) {
                if (_wcls == RETRY_RECONNECT) {
                    file_handle_unlock(h);
                    reconnect_meta_context(rc, "write", NULL);
                    file_handle_lock(h);
                    rc = -EIO;
                }
                if (g_log_errors)
                    syslog(LOG_ERR,
                           "write failed after %d retries (rc=%d/%s)",
                           io_retries, rc, strerror(-rc));
                break;
            }
            io_retries++;
            DBG(1, "nfsfuse: write transient error — waiting %ds (retry %d/%d)\n",
                g_nfs4_retry_wait, io_retries, g_nfs4_retry_max);
            if (g_log_errors)
                syslog(LOG_WARNING, "write error — retry %d/%d (%s)",
                       io_retries, g_nfs4_retry_max,
                       reconnect_reason(rc));
            file_handle_unlock(h);
            sleep(g_nfs4_retry_wait);
            file_handle_lock(h);
        }

        if (rc < 0) {
            if (done == 0) {
                log_nfs_error("write", NULL, rc, file_handle_ctx(h));
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

    DBG(2, "nfsfuse: getattr %s\n", path ? path : "(null)");

    if (fi && fi->fh) {
        struct file_handle *h = (struct file_handle *)(uintptr_t)fi->fh;

        file_handle_lock(h);
        rc = nfs_fstat64(file_handle_ctx(h), h->fh, &st);
        file_handle_unlock(h);
    } else {
        if (path == NULL)
            return -EINVAL;

        META_RETRY_ASYNC(rc, "getattr", path,
            nfs_lstat64(g_state.meta_nfs, path, &st),
            async_nfs_lstat64(g_state.meta_nfs, path, &st));

        /*
         * If the NFS server returns NOENT but we have a deferred-close
         * handle for this path, the file still exists — use fstat on
         * the held-open handle instead.  This covers the case where the
         * server evicts the file from its name cache after CLOSE but
         * the file is still accessible via the open stateid.
         */
        if (rc == -ENOENT && deferred_close_fstat(path, &st)) {
            DBG(1, "nfsfuse: getattr %s recovered via deferred handle\n",
                path);
            rc = 0;
        }
    }

    if (rc < 0)
        return nfs_err_log(rc, "getattr", path, g_state.meta_nfs);

    fill_stat_from_nfs64(stbuf, &st);
    DBG(3, "nfsfuse: getattr %s -> mode=%o size=%lld uid=%d gid=%d\n",
        path ? path : "(null)", (int)stbuf->st_mode,
        (long long)stbuf->st_size, (int)stbuf->st_uid, (int)stbuf->st_gid);
    return 0;
}

static int nfuse_opendir(const char *path, struct fuse_file_info *fi)
{
    struct dir_handle *h = NULL;
    struct nfs_context *ctx = NULL;
    struct nfsdir *dir = NULL;
    int rc;
    int use_private_ctx = g_state.max_mode && !g_state.safe_v4_mode;

    DBG(2, "nfsfuse: opendir %s\n", path ? path : "(null)");

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
            log_nfs_error("opendir", path, rc, ctx);
            destroy_nfs_context_safe(ctx);
            free(h);
            return nfs_err(rc);
        }

        h->ctx = ctx;
        h->own_ctx = 1;
    } else {
        META_RETRY(rc, "opendir", path,
            nfs_opendir(g_state.meta_nfs, path, &dir));

        if (rc < 0) {
            free(h);
            return nfs_err_log(rc, "opendir", path, g_state.meta_nfs);
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

    DBG(2, "nfsfuse: readdir %s off=%lld\n", path ? path : "(null)", (long long)off);

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

    DBG(2, "nfsfuse: releasedir %s\n", path ? path : "(null)");

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

    DBG(2, "nfsfuse: open %s\n", path ? path : "(null)");
    DBG(3, "nfsfuse: open %s flags=0x%x\n", path ? path : "(null)", flags);

    /*
     * Do NOT release the deferred close before the open attempt.
     * The deferred handle keeps the NFS4 OPEN state alive on the
     * server.  If we close it first and the server evicted the file
     * from its name cache, the fresh open will fail with ENOENT.
     * Only release after the fresh open succeeds.
     */
    {
        int retries = 0, did_reconnect = 0;
        for (;;) {
            rc = open_file_handle_common(path, flags, 0, 0, &h);
            if (rc >= 0)
                break;
            int cls = classify_nfs_error(rc);
            if (cls == RETRY_NONE || retries >= g_nfs4_retry_max)
                break;
            retries++;
            if (cls == RETRY_RECONNECT && !did_reconnect) {
                if (reconnect_meta_context(rc, "open", path) == 0)
                    did_reconnect = 1;
                else
                    break;
            } else {
                DBG(1, "nfsfuse: open %s — waiting %ds (retry %d/%d)\n",
                    path ? path : "", g_nfs4_retry_wait, retries, g_nfs4_retry_max);
                sleep(g_nfs4_retry_wait);
            }
        }
    }

    if (rc >= 0) {
        /* Fresh open succeeded — release the deferred handle */
        deferred_close_release(path);
    } else if (rc == -ENOENT) {
        /*
         * Fresh open failed with ENOENT but we may have a deferred
         * handle.  Peek at it (don't remove from pool!) and create a
         * file_handle that shares the NFS handle.  The pool keeps its
         * entry active so that subsequent getattr calls still recover
         * via deferred_close_fstat.  The pool owns the NFS handle —
         * the promoted file_handle must NOT close it on release.
         */
        struct nfs_context *dc_ctx = NULL;
        struct nfsfh *dc_fh = NULL;

        if (deferred_close_peek(path, &dc_ctx, &dc_fh)) {
            DBG(1, "nfsfuse: open %s — promoting deferred handle\n",
                path ? path : "");
            if (g_log_errors)
                syslog(LOG_NOTICE, "open %s — promoting deferred handle",
                       path ? path : "");

            h = calloc(1, sizeof(*h));
            if (h != NULL) {
                h->ctx = dc_ctx;
                h->fh = dc_fh;
                h->own_ctx = 0;
                h->writable = 0;
                h->borrowed = 1;  /* pool owns the NFS handle */
                if (pthread_mutex_init(&h->lock, NULL) == 0) {
                    rc = 0;  /* success — using promoted handle */
                } else {
                    free(h);
                    h = NULL;
                }
            }
        }
    }

    if (rc < 0)
        return rc;

    fi->fh = (uint64_t)(uintptr_t)h;

    fi->keep_cache = 0;
    fi->direct_io = 0;

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

    DBG(2, "nfsfuse: create %s\n", path ? path : "(null)");
    DBG(3, "nfsfuse: create %s mode=%o flags=0x%x\n",
        path ? path : "(null)", (int)mode, flags);

    {
        int retries = 0, did_reconnect = 0;
        for (;;) {
            rc = open_file_handle_common(path, flags, mode, 1, &h);
            if (rc >= 0)
                break;
            int cls = classify_nfs_error(rc);
            if (cls == RETRY_NONE || retries >= g_nfs4_retry_max)
                break;
            retries++;
            if (cls == RETRY_RECONNECT && !did_reconnect) {
                if (reconnect_meta_context(rc, "create", path) == 0)
                    did_reconnect = 1;
                else
                    break;
            } else {
                DBG(1, "nfsfuse: create %s — waiting %ds (retry %d/%d)\n",
                    path ? path : "", g_nfs4_retry_wait, retries, g_nfs4_retry_max);
                sleep(g_nfs4_retry_wait);
            }
        }
    }
    if (rc < 0)
        return rc;

    fi->fh = (uint64_t)(uintptr_t)h;

    fi->keep_cache = 0;
    fi->direct_io = 0;

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

    DBG(2, "nfsfuse: release %s\n", path ? path : "(null)");

    if (h == NULL)
        return 0;

    file_handle_lock(h);
    if (h->writable)
        nfs_fsync(file_handle_ctx(h), h->fh);

    /*
     * Deferred close for NFS4: keep the file handle open to maintain
     * the NFS4 OPEN state on the server, preventing it from evicting
     * the file from its name cache.  This mimics the kernel NFS
     * client's close-to-open consistency behavior.  Only for writable
     * files on the shared context (the ISO creation path).
     */
    if (h->borrowed) {
        /* Promoted from deferred pool — pool owns the NFS handle */
        file_handle_unlock(h);
    } else if (h->writable && !h->own_ctx && g_state.safe_v4_mode && path) {
        file_handle_unlock(h);
        deferred_close_add(path, file_handle_ctx(h), h->fh);
        /* Don't close the NFS handle — it's now owned by deferred pool */
    } else {
        nfs_close(file_handle_ctx(h), h->fh);
        file_handle_unlock(h);
        if (h->own_ctx)
            destroy_nfs_context_safe(h->ctx);
    }

    pthread_mutex_destroy(&h->lock);
    free(h);
    fi->fh = 0;
    return 0;
}

static int nfuse_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi)
{
    struct file_handle *h = (struct file_handle *)(uintptr_t)fi->fh;
    int rc;

    (void)path;

    DBG(3, "nfsfuse: read %s size=%zu off=%lld\n",
        path ? path : "(null)", size, (long long)offset);

    if (h == NULL)
        return -EBADF;

    rc = pread_full(h, buf, size, offset);
    DBG(3, "nfsfuse: read %s -> %d\n", path ? path : "(null)", rc);
    return rc;
}

static int nfuse_write(const char *path, const char *buf, size_t size, off_t offset,
                       struct fuse_file_info *fi)
{
    struct file_handle *h = (struct file_handle *)(uintptr_t)fi->fh;
    int rc;

    (void)path;

    DBG(3, "nfsfuse: write %s size=%zu off=%lld\n",
        path ? path : "(null)", size, (long long)offset);

    if (h == NULL)
        return -EBADF;

    rc = pwrite_full(h, buf, size, offset);
    DBG(3, "nfsfuse: write %s -> %d\n", path ? path : "(null)", rc);
    return rc;
}

static int nfuse_truncate(const char *path, off_t size, struct fuse_file_info *fi)
{
    int rc;

    DBG(2, "nfsfuse: truncate %s\n", path ? path : "(null)");
    DBG(3, "nfsfuse: truncate %s size=%lld\n",
        path ? path : "(null)", (long long)size);

    if (fi && fi->fh) {
        struct file_handle *h = (struct file_handle *)(uintptr_t)fi->fh;

        file_handle_lock(h);
        rc = nfs_ftruncate(file_handle_ctx(h), h->fh, (uint64_t)size);
        file_handle_unlock(h);
    } else {
        if (path == NULL)
            return -EINVAL;

        META_RETRY(rc, "truncate", path,
            nfs_truncate(g_state.meta_nfs, path, (uint64_t)size));
    }

    if (rc < 0)
        return nfs_err_log(rc, "truncate", path, g_state.meta_nfs);

    return 0;
}

static int nfuse_utimens(const char *path, const struct timespec tv[2],
                         struct fuse_file_info *fi)
{
    struct timeval times[2];
    struct timeval now;
    int rc;
    int need_stat;

    (void)fi;

    DBG(2, "nfsfuse: utimens %s\n", path ? path : "(null)");

    if (path == NULL)
        return -EINVAL;

    gettimeofday(&now, NULL);

    if (tv == NULL) {
        times[0] = now;
        times[1] = now;
    } else {
        need_stat = (tv[0].tv_nsec == UTIME_OMIT) ||
                    (tv[1].tv_nsec == UTIME_OMIT);

        if (need_stat) {
            struct nfs_stat_64 st;

            META_RETRY(rc, "utimens", path,
                nfs_lstat64(g_state.meta_nfs, path, &st));

            if (rc < 0)
                return nfs_err_log(rc, "utimens", path, g_state.meta_nfs);

            if (tv[0].tv_nsec == UTIME_OMIT) {
                times[0].tv_sec = (time_t)st.nfs_atime;
                times[0].tv_usec = (suseconds_t)(st.nfs_atime_nsec / 1000);
            }
            if (tv[1].tv_nsec == UTIME_OMIT) {
                times[1].tv_sec = (time_t)st.nfs_mtime;
                times[1].tv_usec = (suseconds_t)(st.nfs_mtime_nsec / 1000);
            }
        }

        if (tv[0].tv_nsec == UTIME_NOW) {
            times[0] = now;
        } else if (tv[0].tv_nsec != UTIME_OMIT) {
            times[0].tv_sec = tv[0].tv_sec;
            times[0].tv_usec = (suseconds_t)(tv[0].tv_nsec / 1000);
        }

        if (tv[1].tv_nsec == UTIME_NOW) {
            times[1] = now;
        } else if (tv[1].tv_nsec != UTIME_OMIT) {
            times[1].tv_sec = tv[1].tv_sec;
            times[1].tv_usec = (suseconds_t)(tv[1].tv_nsec / 1000);
        }
    }

    META_RETRY(rc, "utimens", path,
        nfs_utimes(g_state.meta_nfs, path, times));

    if (rc < 0)
        return nfs_err_log(rc, "utimens", path, g_state.meta_nfs);

    return 0;
}

static int nfuse_unlink(const char *path)
{
    int rc;

    DBG(2, "nfsfuse: unlink %s\n", path ? path : "(null)");
    DBG(3, "nfsfuse: unlink %s\n", path ? path : "(null)");

    META_RETRY(rc, "unlink", path,
        nfs_unlink(g_state.meta_nfs, path));

    if (rc < 0)
        return nfs_err_log(rc, "unlink", path, g_state.meta_nfs);

    return 0;
}

static int nfuse_mkdir(const char *path, mode_t mode)
{
    int rc;

    (void)mode;

    DBG(2, "nfsfuse: mkdir %s\n", path ? path : "(null)");
    DBG(3, "nfsfuse: mkdir %s mode=%o\n", path ? path : "(null)", (int)mode);

    META_RETRY(rc, "mkdir", path,
        CALL_NFS_MKDIR(g_state.meta_nfs, path, mode));

    if (rc < 0)
        return nfs_err_log(rc, "mkdir", path, g_state.meta_nfs);

    return 0;
}

static int nfuse_rmdir(const char *path)
{
    int rc;

    DBG(2, "nfsfuse: rmdir %s\n", path ? path : "(null)");
    DBG(3, "nfsfuse: rmdir %s\n", path ? path : "(null)");

    META_RETRY(rc, "rmdir", path,
        nfs_rmdir(g_state.meta_nfs, path));

    if (rc < 0)
        return nfs_err_log(rc, "rmdir", path, g_state.meta_nfs);

    return 0;
}

static int nfuse_rename(const char *from, const char *to, unsigned int flags)
{
    int rc;

    DBG(2, "nfsfuse: rename %s -> %s\n",
        from ? from : "(null)", to ? to : "(null)");

    if (flags != 0)
        return -EINVAL;

    META_RETRY(rc, "rename", from,
        nfs_rename(g_state.meta_nfs, from, to));

    if (rc < 0)
        return nfs_err_log(rc, "rename", from, g_state.meta_nfs);

    return 0;
}

static int nfuse_readlink(const char *path, char *buf, size_t size)
{
    int rc;

    DBG(2, "nfsfuse: readlink %s\n", path ? path : "(null)");

    if (path == NULL || buf == NULL || size == 0)
        return -EINVAL;

    META_RETRY(rc, "readlink", path,
        nfs_readlink(g_state.meta_nfs, path, buf, (int)size));

    if (rc < 0)
        return nfs_err_log(rc, "readlink", path, g_state.meta_nfs);

    buf[size - 1] = '\0';
    return 0;
}

static int nfuse_symlink(const char *target, const char *linkpath)
{
    int rc;

    DBG(2, "nfsfuse: symlink %s -> %s\n",
        linkpath ? linkpath : "(null)", target ? target : "(null)");

    META_RETRY(rc, "symlink", linkpath,
        nfs_symlink(g_state.meta_nfs, target, linkpath));

    if (rc < 0)
        return nfs_err_log(rc, "symlink", linkpath, g_state.meta_nfs);

    return 0;
}

static int nfuse_link(const char *oldpath, const char *newpath)
{
    int rc;

    DBG(2, "nfsfuse: link %s -> %s\n",
        newpath ? newpath : "(null)", oldpath ? oldpath : "(null)");

    META_RETRY(rc, "link", newpath,
        nfs_link(g_state.meta_nfs, oldpath, newpath));

    if (rc < 0)
        return nfs_err_log(rc, "link", newpath, g_state.meta_nfs);

    return 0;
}

static int nfuse_chmod(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    int rc;

    DBG(2, "nfsfuse: chmod %s\n", path ? path : "(null)");
    DBG(3, "nfsfuse: chmod %s mode=%o\n", path ? path : "(null)", (int)mode);

    if (fi && fi->fh) {
        struct file_handle *h = (struct file_handle *)(uintptr_t)fi->fh;

        file_handle_lock(h);
        rc = nfs_fchmod(file_handle_ctx(h), h->fh, mode);
        file_handle_unlock(h);
    } else {
        if (path == NULL)
            return -EINVAL;

        META_RETRY(rc, "chmod", path,
            nfs_chmod(g_state.meta_nfs, path, mode));
    }

    if (rc < 0)
        return nfs_err_log(rc, "chmod", path, g_state.meta_nfs);

    return 0;
}

static int nfuse_chown(const char *path, uid_t uid, gid_t gid,
                       struct fuse_file_info *fi)
{
    int rc;

    DBG(2, "nfsfuse: chown %s\n", path ? path : "(null)");
    DBG(3, "nfsfuse: chown %s uid=%d gid=%d\n",
        path ? path : "(null)", (int)uid, (int)gid);

    if (fi && fi->fh) {
        struct file_handle *h = (struct file_handle *)(uintptr_t)fi->fh;

        file_handle_lock(h);
        rc = nfs_fchown(file_handle_ctx(h), h->fh, uid, gid);
        file_handle_unlock(h);
    } else {
        if (path == NULL)
            return -EINVAL;

        META_RETRY(rc, "chown", path,
            nfs_chown(g_state.meta_nfs, path, uid, gid));
    }

    if (rc < 0)
        return nfs_err_log(rc, "chown", path, g_state.meta_nfs);

    return 0;
}

static int nfuse_mknod(const char *path, mode_t mode, dev_t rdev)
{
    int rc;

    DBG(2, "nfsfuse: mknod %s\n", path ? path : "(null)");
    DBG(3, "nfsfuse: mknod %s mode=%o rdev=%ld\n",
        path ? path : "(null)", (int)mode, (long)rdev);

    META_RETRY(rc, "mknod", path,
        nfs_mknod(g_state.meta_nfs, path, mode, (int)rdev));

    if (rc < 0)
        return nfs_err_log(rc, "mknod", path, g_state.meta_nfs);

    return 0;
}

static int nfuse_access(const char *path, int mask)
{
    int rc;

    DBG(2, "nfsfuse: access %s\n", path ? path : "(null)");
    DBG(3, "nfsfuse: access %s mask=0x%x\n", path ? path : "(null)", mask);

    META_RETRY(rc, "access", path,
        nfs_access(g_state.meta_nfs, path, mask));

    if (rc < 0)
        return nfs_err_log(rc, "access", path, g_state.meta_nfs);

    return 0;
}

static int nfuse_flush(const char *path, struct fuse_file_info *fi)
{
    struct file_handle *h;
    int rc;

    if (fi == NULL || fi->fh == 0)
        return 0;

    h = (struct file_handle *)(uintptr_t)fi->fh;

    if (!h->writable)
        return 0;

    DBG(2, "nfsfuse: flush (fsync) %s\n", path ? path : "(null)");

    file_handle_lock(h);
    rc = nfs_fsync(file_handle_ctx(h), h->fh);
    file_handle_unlock(h);

    if (rc < 0)
        return nfs_err_log(rc, "flush/fsync", path, file_handle_ctx(h));

    return 0;
}

static int nfuse_fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
    struct file_handle *h;
    int rc;

    (void)path;
    (void)datasync;

    DBG(2, "nfsfuse: fsync %s\n", path ? path : "(null)");

    if (fi == NULL || fi->fh == 0)
        return 0;

    h = (struct file_handle *)(uintptr_t)fi->fh;

    file_handle_lock(h);
    rc = nfs_fsync(file_handle_ctx(h), h->fh);
    file_handle_unlock(h);

    if (rc < 0)
        return nfs_err_log(rc, "fsync", path, file_handle_ctx(h));

    return 0;
}

static int nfuse_statfs(const char *path, struct statvfs *st)
{
    int rc;

    (void)path;

    DBG(2, "nfsfuse: statfs\n");

    memset(st, 0, sizeof(*st));

    META_RETRY_ASYNC(rc, "statfs", "/",
        nfs_statvfs(g_state.meta_nfs, "/", st),
        async_nfs_statvfs(g_state.meta_nfs, "/", st));

    if (rc < 0)
        return nfs_err_log(rc, "statfs", "/", g_state.meta_nfs);

    return 0;
}

static void *nfuse_init(struct fuse_conn_info *conn, struct fuse_config *cfg)
{
    g_fuse_instance = fuse_get_context()->fuse;

    /*
     * Start background threads HERE, not in main().
     * fuse_main() daemonizes (forks) before calling init, which kills
     * any pthreads created earlier.  Threads started here survive.
     */
    if (g_state.safe_v4_mode) {
        if (start_keepalive_thread() == 0)
            DBG(1, "nfsfuse: NFS4 keepalive thread started\n");
        else
            DBG(1, "nfsfuse: WARNING: could not start keepalive thread\n");
    }

    if (g_state.has_dead_timeout && g_state.dead_timeout > 0) {
        g_state.last_nfs_success = time(NULL);  /* healthy at startup */
        if (start_dead_timeout_watchdog() == 0)
            DBG(1, "nfsfuse: dead-timeout watchdog thread started (%ds)\n",
                g_state.dead_timeout);
        else
            DBG(1, "nfsfuse: WARNING: could not start watchdog thread\n");
    }

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

        if (g_writeback_cache && (conn->capable & FUSE_CAP_WRITEBACK_CACHE)) {
            conn->want |= FUSE_CAP_WRITEBACK_CACHE;
            DBG(1, "nfsfuse: writeback cache enabled (NFSv4)\n");
        }

        return NULL;
    }

    if (g_state.max_mode) {
        /*
         * Use auto_cache instead of kernel_cache.  kernel_cache never
         * invalidates cached data, so reads can return stale data if
         * the file was modified on the server.  auto_cache checks
         * file attributes (mtime/size) on each open and invalidates
         * the page cache when they change — much safer for NFS.
         *
         * Do NOT enable WRITEBACK_CACHE: it buffers writes in the
         * kernel page cache and flushes asynchronously.  If the NFS
         * server becomes unreachable or the process crashes, buffered
         * writes are silently lost.  Synchronous writes (the default)
         * ensure data reaches the NFS server before returning success.
         */
        cfg->auto_cache = 1;
        cfg->ac_attr_timeout_set = 1;
        cfg->ac_attr_timeout = 3.0;
        cfg->attr_timeout = 3.0;
        cfg->entry_timeout = 5.0;
        cfg->negative_timeout = 3.0;

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
        if (g_writeback_cache && (conn->capable & FUSE_CAP_WRITEBACK_CACHE)) {
            conn->want |= FUSE_CAP_WRITEBACK_CACHE;
            DBG(1, "nfsfuse: writeback cache enabled (NFSv3 max)\n");
        }
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

/*
 * Watchdog thread: if the process hasn't exited within a few seconds
 * of unmount, force-exit.  NFS calls (nfs_close, nfs_lstat64) can
 * block for up to 60 seconds on a dead server, preventing clean exit.
 */
static void *exit_watchdog(void *arg)
{
    (void)arg;
    sleep(5);
    DBG(1, "nfsfuse: exit watchdog — forcing exit\n");
    if (g_log_errors)
        syslog(LOG_NOTICE, "exit watchdog — forcing exit after unmount");
    _exit(0);
    return NULL;
}

static void nfuse_destroy(void *private_data)
{
    pthread_t wd;

    (void)private_data;

    DBG(1, "nfsfuse: unmount — cleaning up\n");

    /* Start a watchdog that force-exits after 5 seconds */
    if (pthread_create(&wd, NULL, exit_watchdog, NULL) == 0)
        pthread_detach(wd);

    stop_keepalive_thread();
    deferred_close_all();
}

static struct fuse_operations nfuse_ops = {
    .init       = nfuse_init,
    .destroy    = nfuse_destroy,

    .getattr    = nfuse_getattr,
    .access     = nfuse_access,
    .readlink   = nfuse_readlink,

    .opendir    = nfuse_opendir,
    .readdir    = nfuse_readdir,
    .releasedir = nfuse_releasedir,

    .open       = nfuse_open,
    .create     = nfuse_create,
    .release    = nfuse_release,
    .mknod      = nfuse_mknod,

    .read       = nfuse_read,
    .write      = nfuse_write,
    .truncate   = nfuse_truncate,
    .utimens    = nfuse_utimens,

    .chmod      = nfuse_chmod,
    .chown      = nfuse_chown,

    .unlink     = nfuse_unlink,
    .mkdir      = nfuse_mkdir,
    .rmdir      = nfuse_rmdir,
    .rename     = nfuse_rename,
    .symlink    = nfuse_symlink,
    .link       = nfuse_link,

    .flush      = nfuse_flush,
    .fsync      = nfuse_fsync,
    .statfs     = nfuse_statfs,
};

static void usage(const char *prog)
{
    fprintf(stderr, "nfsfuse %s (build %s, %s)\n",
            NFSFUSE_VERSION, NFSFUSE_BUILD, NFSFUSE_BUILD_DATE);
    fprintf(stderr,
        "\nUsage:\n"
        "  %s [options] nfs://server/export/path[?version=3|4] <mountpoint> [FUSE options]\n\n"
        "Options:\n"
        "  --max                Enable performance optimizations\n"
        "  --debug [level]      Print debug tracing to stderr (default level 1)\n"
        "                         1 = mount/config/reconnect info\n"
        "                         2 = all FUSE operations (path + result)\n"
        "                         3 = detailed parameters (offsets, sizes, flags, modes)\n"
        "  --debug-output <dst> Send debug output to <dst> instead of stderr\n"
        "                         syslog = write to syslog (daemon.debug)\n"
        "                         <path> = write to file (e.g. /var/log/nfsfuse.log)\n"
        "  --log-errors         Log NFS errors to syslog (daemon facility)\n"
        "  --noatime            Do not update access time on read\n"
        "  --nodiratime         Do not update directory access time\n"
        "  --noexec             Disallow execution of binaries on mount\n"
        "  --do-not-reconnect-on-stale    Disable auto-reconnect on ESTALE (on by default)\n"
        "  --do-not-reconnect-on-io-error Disable auto-reconnect on EIO (on by default)\n"
        "  --async              Use async NFS event loop (prevents stuck-call freezes)\n"
        "  --auto-remount       Re-exec on dead-timeout instead of just dying\n"
        "  --writeback-cache    Enable kernel writeback cache (faster writes,\n"
        "                       risk of data loss on crash — see docs)\n"
        "  --version            Show version information\n\n"
        "Timeout and retry options:\n"
        "  --timeout <ms>       RPC request timeout in milliseconds (default: 10000)\n"
        "  --retrans <n>        RPC retransmission attempts before major timeout (default: 0)\n"
        "  --autoreconnect <n>  TCP reconnect attempts on disconnect (-1=infinite, default: 0)\n"
        "  --tcp-syncnt <n>     TCP SYN retry count for connection establishment\n"
        "  --poll-timeout <ms>  Poll interval in milliseconds between response checks\n"
        "  --dead-timeout <s>   Unmount after N seconds of consecutive failures (default: disabled)\n"
        "  --stuck-timeout <s>  Recover stuck NFS calls by shutting down socket (default: disabled)\n"
        "  --nfs4-retries <n>   NFS4 error retry attempts (default: 5)\n"
        "  --nfs4-retry-wait <s> Seconds between NFS4 retries (default: 30)\n\n"
        "Examples:\n"
        "  %s 'nfs://192.168.52.200/store001/cdimage?version=3' /mnt/nfs\n"
        "  %s --timeout 30000 --autoreconnect -1 'nfs://10.0.0.1/data' /mnt/nfs\n"
        "  %s --retrans 5 --tcp-syncnt 3 'nfs://10.0.0.1/data?version=4' /mnt/nfs\n",
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

static int is_nfsfuse_opt(const char *arg)
{
    return strcmp(arg, "--max") == 0 ||
           strcmp(arg, "--debug") == 0 ||
           strcmp(arg, "--debug-output") == 0 ||
           strcmp(arg, "--log-errors") == 0 ||
           strcmp(arg, "--noatime") == 0 ||
           strcmp(arg, "--nodiratime") == 0 ||
           strcmp(arg, "--noexec") == 0 ||
           strcmp(arg, "--reconnect-on-stale") == 0 ||
           strcmp(arg, "--reconnect-on-io-error") == 0 ||
           strcmp(arg, "--do-not-reconnect-on-stale") == 0 ||
           strcmp(arg, "--do-not-reconnect-on-io-error") == 0 ||
           strcmp(arg, "--writeback-cache") == 0 ||
           strcmp(arg, "--async") == 0 ||
           strcmp(arg, "--auto-remount") == 0 ||
           strcmp(arg, "--timeout") == 0 ||
           strcmp(arg, "--retrans") == 0 ||
           strcmp(arg, "--autoreconnect") == 0 ||
           strcmp(arg, "--tcp-syncnt") == 0 ||
           strcmp(arg, "--poll-timeout") == 0 ||
           strcmp(arg, "--dead-timeout") == 0 ||
           strcmp(arg, "--stuck-timeout") == 0 ||
           strcmp(arg, "--nfs4-retries") == 0 ||
           strcmp(arg, "--nfs4-retry-wait") == 0;
}

static int is_nfsfuse_opt_with_value(const char *arg)
{
    return strcmp(arg, "--timeout") == 0 ||
           strcmp(arg, "--retrans") == 0 ||
           strcmp(arg, "--autoreconnect") == 0 ||
           strcmp(arg, "--tcp-syncnt") == 0 ||
           strcmp(arg, "--poll-timeout") == 0 ||
           strcmp(arg, "--dead-timeout") == 0 ||
           strcmp(arg, "--stuck-timeout") == 0 ||
           strcmp(arg, "--nfs4-retries") == 0 ||
           strcmp(arg, "--nfs4-retry-wait") == 0 ||
           strcmp(arg, "--debug-output") == 0;
}

static int is_debug_level(const char *s)
{
    return s != NULL && s[0] >= '1' && s[0] <= '9' && s[1] == '\0';
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

    /* Save for --auto-remount (re-exec on dead-timeout) */
    g_saved_argc = argc;
    g_saved_argv = argv;
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
        if (strcmp(argv[i], "--debug") == 0) {
            g_debug = 1;
            if (i + 1 < argc && is_debug_level(argv[i + 1]))
                g_debug = atoi(argv[++i]);
        }
        if (strcmp(argv[i], "--debug-output") == 0 && i + 1 < argc) {
            i++;
            if (strcmp(argv[i], "syslog") == 0)
                g_debug_syslog = 1;
            else {
                g_debug_file = fopen(argv[i], "a");
                if (g_debug_file == NULL) {
                    fprintf(stderr, "nfsfuse: cannot open debug output file: %s: %s\n",
                            argv[i], strerror(errno));
                    return 1;
                }
                setlinebuf(g_debug_file);
            }
        }
        if (strcmp(argv[i], "--log-errors") == 0)
            g_log_errors = 1;
        if (strcmp(argv[i], "--noatime") == 0)
            g_noatime = 1;
        if (strcmp(argv[i], "--nodiratime") == 0)
            g_nodiratime = 1;
        if (strcmp(argv[i], "--noexec") == 0)
            g_noexec = 1;
        if (strcmp(argv[i], "--reconnect-on-stale") == 0)
            g_reconnect_on_stale = 1;  /* already default, kept for compat */
        if (strcmp(argv[i], "--reconnect-on-io-error") == 0)
            g_reconnect_on_io_error = 1;  /* already default, kept for compat */
        if (strcmp(argv[i], "--do-not-reconnect-on-stale") == 0)
            g_reconnect_on_stale = 0;
        if (strcmp(argv[i], "--do-not-reconnect-on-io-error") == 0)
            g_reconnect_on_io_error = 0;
        if (strcmp(argv[i], "--writeback-cache") == 0)
            g_writeback_cache = 1;
        if (strcmp(argv[i], "--async") == 0)
            g_async_mode = 1;
        if (strcmp(argv[i], "--auto-remount") == 0)
            g_auto_remount = 1;
        if (is_nfsfuse_opt_with_value(argv[i]) && i + 1 < argc)
            i++;
    }

    if (argc < 3) {
        usage(argv[0]);
        return 1;
    }

    if (g_debug)
        print_version();

    /* syslog is opened later, after mount_idx is known */

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
        if (strcmp(argv[i], "--auto-remount") == 0)
            continue;
        if (strcmp(argv[i], "--async") == 0)
            continue;
        if (strcmp(argv[i], "--auto-remount") == 0)
            continue;
        }
        if (strcmp(argv[i], "--debug") == 0) {
            if (i + 1 < argc && is_debug_level(argv[i + 1]))
                i++;
            continue;
        }
        if (strcmp(argv[i], "--debug-output") == 0 && i + 1 < argc) {
            i++;
            continue;
        }
        if (strcmp(argv[i], "--log-errors") == 0)
            continue;
        if (strcmp(argv[i], "--noatime") == 0)
            continue;
        if (strcmp(argv[i], "--nodiratime") == 0)
            continue;
        if (strcmp(argv[i], "--noexec") == 0)
            continue;
        if (strcmp(argv[i], "--reconnect-on-stale") == 0)
            continue;
        if (strcmp(argv[i], "--reconnect-on-io-error") == 0)
            continue;
        if (strcmp(argv[i], "--do-not-reconnect-on-stale") == 0)
            continue;
        if (strcmp(argv[i], "--do-not-reconnect-on-io-error") == 0)
            continue;
        if (strcmp(argv[i], "--writeback-cache") == 0)
            continue;
        if (strcmp(argv[i], "--async") == 0)
            continue;
        if (strcmp(argv[i], "--auto-remount") == 0)
            continue;

        if (strcmp(argv[i], "--timeout") == 0 && i + 1 < argc) {
            g_state.timeout = atoi(argv[++i]);
            g_state.has_timeout = 1;
            continue;
        }
        if (strcmp(argv[i], "--retrans") == 0 && i + 1 < argc) {
            g_state.retrans = atoi(argv[++i]);
            g_state.has_retrans = 1;
            continue;
        }
        if (strcmp(argv[i], "--autoreconnect") == 0 && i + 1 < argc) {
            g_state.autoreconnect = atoi(argv[++i]);
            g_state.has_autoreconnect = 1;
            continue;
        }
        if (strcmp(argv[i], "--tcp-syncnt") == 0 && i + 1 < argc) {
            g_state.tcp_syncnt = atoi(argv[++i]);
            g_state.has_tcp_syncnt = 1;
            continue;
        }
        if (strcmp(argv[i], "--poll-timeout") == 0 && i + 1 < argc) {
            g_state.poll_timeout = atoi(argv[++i]);
            g_state.has_poll_timeout = 1;
            continue;
        }
        if (strcmp(argv[i], "--dead-timeout") == 0 && i + 1 < argc) {
            g_state.dead_timeout = atoi(argv[++i]);
            g_state.has_dead_timeout = 1;
            continue;
        }
        if (strcmp(argv[i], "--stuck-timeout") == 0 && i + 1 < argc) {
            g_state.stuck_timeout = atoi(argv[++i]);
            continue;
        }
        if (strcmp(argv[i], "--nfs4-retries") == 0 && i + 1 < argc) {
            g_nfs4_retry_max = atoi(argv[++i]);
            continue;
        }
        if (strcmp(argv[i], "--nfs4-retry-wait") == 0 && i + 1 < argc) {
            g_nfs4_retry_wait = atoi(argv[++i]);
            continue;
        }

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

    /* Store mountpoint for debug log prefix and syslog ident */
    g_mountpoint = argv[mount_idx];

    if (g_log_errors || g_debug_syslog) {
        snprintf(g_syslog_ident, sizeof(g_syslog_ident),
                 "nfsfuse[%s]", g_mountpoint);
        openlog(g_syslog_ident, LOG_PID, LOG_DAEMON);
        g_syslog_open = 1;
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

    DBG(1, "nfsfuse: url=%s v4=%d max=%d\n",
        g_state.url_effective, g_state.safe_v4_mode, g_state.max_mode);

    g_state.fsname = build_fsname_from_url(g_state.url_base);
    if (g_state.fsname == NULL)
        g_state.fsname = xstrdup("nfsfuse");

    if (g_state.has_dead_timeout)
        DBG(1, "nfsfuse: dead-timeout=%ds\n", g_state.dead_timeout);

    DBG(1, "nfsfuse: mounting...\n");

    g_state.meta_nfs = mount_new_context(g_state.url_effective);
    if (g_state.meta_nfs == NULL) {
        fprintf(stderr, "nfsfuse: mount failed for %s\n", g_state.url_effective);
        cleanup_app_state();
        return 1;
    }

    readmax = nfs_get_readmax(g_state.meta_nfs);
    writemax = nfs_get_writemax(g_state.meta_nfs);

    DBG(1, "nfsfuse: mounted, readmax=%zu writemax=%zu\n", readmax, writemax);

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

    if (g_noatime) {
        if (add_fuse_arg(&fuse_argv, &fuse_argc, "-o") != 0 ||
            add_fuse_arg(&fuse_argv, &fuse_argc, "noatime") != 0) {
            free_fuse_args(fuse_argv, fuse_argc);
            cleanup_app_state();
            return 1;
        }
    }

    if (g_nodiratime) {
        if (add_fuse_arg(&fuse_argv, &fuse_argc, "-o") != 0 ||
            add_fuse_arg(&fuse_argv, &fuse_argc, "nodiratime") != 0) {
            free_fuse_args(fuse_argv, fuse_argc);
            cleanup_app_state();
            return 1;
        }
    }

    if (g_noexec) {
        if (add_fuse_arg(&fuse_argv, &fuse_argc, "-o") != 0 ||
            add_fuse_arg(&fuse_argv, &fuse_argc, "noexec") != 0) {
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
        if (is_nfsfuse_opt(argv[i])) {
            if (is_nfsfuse_opt_with_value(argv[i]))
                i++;
            else if (strcmp(argv[i], "--debug") == 0 &&
                     i + 1 < argc && is_debug_level(argv[i + 1]))
                i++;
            continue;
        }

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

    fprintf(stderr, "nfsfuse %s (build %s, %s): %s mounted on %s (nfsv%s%s%s)\n",
            NFSFUSE_VERSION, NFSFUSE_BUILD, NFSFUSE_BUILD_DATE,
            g_state.fsname ? g_state.fsname : argv[url_idx],
            argv[mount_idx],
            g_state.safe_v4_mode ? "4" : "3",
            g_state.max_mode ? ", max" : "",
            g_writeback_cache ? ", writeback" : "");

    if (g_debug) {
        DBG(1, "nfsfuse: starting fuse (argc=%d)\n", fuse_argc);
        for (i = 0; i < fuse_argc; i++)
            DBG(1, "  argv[%d]=%s\n", i, fuse_argv[i]);
    }

    /*
     * NOTE: threads are started in nfuse_init(), not here.
     * fuse_main() daemonizes (forks) by default, which destroys all
     * pthreads.  nfuse_init() runs after daemonization, so threads
     * created there survive.
     */

    rc = fuse_main(fuse_argc, fuse_argv, &nfuse_ops, NULL);

    free_fuse_args(fuse_argv, fuse_argc);

    if (g_state.meta_nfs != NULL || g_state.url_base != NULL ||
        g_state.url_effective != NULL || g_state.fsname != NULL)
        cleanup_app_state();

    if (g_debug_file) {
        fclose(g_debug_file);
        g_debug_file = NULL;
    }

    if (g_syslog_open)
        closelog();

    return rc;
}
