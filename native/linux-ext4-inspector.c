#define _GNU_SOURCE

#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#if defined(__linux__) && defined(__has_include)
#if __has_include(<fcntl.h>) && __has_include(<linux/fs.h>) &&                 \
    __has_include(<linux/close_range.h>) && __has_include(<linux/loop.h>) && \
    __has_include(<linux/magic.h>) &&                                       \
    __has_include(<linux/openat2.h>) && __has_include(<sys/ioctl.h>) &&      \
    __has_include(<sys/mount.h>) && __has_include(<sys/prctl.h>) &&          \
    __has_include(<sys/syscall.h>) &&                                       \
    __has_include(<sys/sysmacros.h>) && __has_include(<sys/vfs.h>) &&        \
    __has_include(<sys/wait.h>) && __has_include(<sys/xattr.h>) &&           \
    __has_include(<unistd.h>)
#define PORTABLE_CODEX_HAS_LINUX_HEADERS 1
#endif
#if __has_include(<linux/ext4.h>)
#define PORTABLE_CODEX_HAS_LINUX_EXT4_HEADER 1
#endif
#elif defined(__linux__)
#define PORTABLE_CODEX_HAS_LINUX_HEADERS 1
#endif

#if defined(PORTABLE_CODEX_HAS_LINUX_HEADERS)
#include <fcntl.h>
#include <inttypes.h>
#include <linux/close_range.h>
#if defined(PORTABLE_CODEX_HAS_LINUX_EXT4_HEADER)
#include <linux/ext4.h>
#endif
#include <linux/fs.h>
#include <linux/loop.h>
#include <linux/magic.h>
#include <linux/openat2.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/sysmacros.h>
#include <sys/vfs.h>
#include <sys/xattr.h>
#endif

enum inspector_exit_code {
  INSPECTOR_EXIT_USAGE = 64,
  INSPECTOR_EXIT_MISMATCH = 65,
  INSPECTOR_EXIT_MISSING = 66,
  INSPECTOR_EXIT_UNSUPPORTED = 69,
  INSPECTOR_EXIT_EXISTS = 73,
  INSPECTOR_EXIT_IO = 74,
  INSPECTOR_EXIT_UNREADABLE = 77,
  INSPECTOR_EXIT_OUTCOME_UNCERTAIN = 78,
};

#if defined(PORTABLE_CODEX_HAS_LINUX_HEADERS) &&                             \
    (defined(EXT4_IOC_GETFSUUID) || defined(FS_IOC_GETFSUUID)) &&            \
    defined(EXT4_SUPER_MAGIC) && defined(O_PATH) &&                          \
    defined(AT_EMPTY_PATH) &&                                                \
    defined(RESOLVE_BENEATH) && defined(RESOLVE_NO_MAGICLINKS) &&            \
    defined(RESOLVE_NO_SYMLINKS) && defined(RESOLVE_NO_XDEV) &&              \
    defined(LOOP_CONFIGURE) && defined(LOOP_CTL_GET_FREE) &&                 \
    defined(BLKGETSIZE64) && defined(BLKSSZGET) && defined(BLKGETDISKSEQ) && \
    defined(CLOSE_RANGE_UNSHARE) && defined(STATX_MNT_ID) &&                \
    defined(UMOUNT_NOFOLLOW) && defined(PR_SET_PDEATHSIG) &&                 \
    defined(PR_SET_CHILD_SUBREAPER) &&                                       \
    (defined(SYS_close_range) || defined(__NR_close_range)) &&              \
    (defined(SYS_openat2) || defined(__NR_openat2))
#define PORTABLE_CODEX_HAS_EXT4_INSPECTION_ABI 1
#endif

#if defined(PORTABLE_CODEX_HAS_EXT4_INSPECTION_ABI) ||                      \
    defined(PORTABLE_CODEX_FORMATTER_SUPERVISOR_TEST)

#define FORMATTER_OPERATION_TIMEOUT_MS UINT64_C(47000)
#define FORMATTER_EXECUTION_TIMEOUT_MS UINT64_C(40000)
#define FORMATTER_TERM_GRACE_MS UINT64_C(2000)
#define FORMATTER_KILL_REAP_TIMEOUT_MS UINT64_C(5000)
#define FORMATTER_OUTER_RESERVE_MS UINT64_C(10000)
#define FORMATTER_WAIT_POLL_MS UINT64_C(5)

_Static_assert(FORMATTER_EXECUTION_TIMEOUT_MS + FORMATTER_TERM_GRACE_MS +
                       FORMATTER_KILL_REAP_TIMEOUT_MS ==
                   FORMATTER_OPERATION_TIMEOUT_MS,
               "formatter phases must fill the operation deadline");
_Static_assert(FORMATTER_OPERATION_TIMEOUT_MS + FORMATTER_OUTER_RESERVE_MS <
                   UINT64_C(60000),
               "formatter supervision must precede the JS helper timeout");

enum formatter_supervision_result {
  FORMATTER_SUPERVISION_ERROR = -1,
  FORMATTER_SUPERVISION_REAPED = 0,
  FORMATTER_SUPERVISION_TIMED_OUT = 1,
};

static int monotonic_milliseconds(uint64_t *output) {
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0) return -1;
  if ((uint64_t)now.tv_sec > (UINT64_MAX - (uint64_t)now.tv_nsec / 1000000U) /
                                 UINT64_C(1000)) {
    errno = EOVERFLOW;
    return -1;
  }
  *output = (uint64_t)now.tv_sec * UINT64_C(1000) +
            (uint64_t)now.tv_nsec / UINT64_C(1000000);
  return 0;
}

static int deadline_after(uint64_t duration_ms, uint64_t *deadline) {
  uint64_t now;
  if (monotonic_milliseconds(&now) != 0 || now > UINT64_MAX - duration_ms) {
    errno = EOVERFLOW;
    return -1;
  }
  *deadline = now + duration_ms;
  return 0;
}

static int phase_deadline(uint64_t hard_deadline, uint64_t phase_ms,
                          uint64_t reserved_ms, uint64_t *deadline) {
  uint64_t now;
  uint64_t phase_end;
  uint64_t latest_end;
  if (reserved_ms >= hard_deadline ||
      monotonic_milliseconds(&now) != 0 || now >= hard_deadline - reserved_ms ||
      now > UINT64_MAX - phase_ms) {
    errno = ETIMEDOUT;
    return -1;
  }
  phase_end = now + phase_ms;
  latest_end = hard_deadline - reserved_ms;
  *deadline = phase_end < latest_end ? phase_end : latest_end;
  return 0;
}

static int formatter_child_terminal_until(pid_t child, uint64_t deadline) {
  for (;;) {
    siginfo_t information;
    uint64_t now;
    uint64_t remaining;
    uint64_t sleep_ms;
    struct timespec delay;
    int wait_result;

    memset(&information, 0, sizeof(information));
    wait_result = waitid(P_PID, (id_t)child, &information,
                         WEXITED | WNOHANG | WNOWAIT);
    if (wait_result != 0) {
      if (errno != EINTR || monotonic_milliseconds(&now) != 0) return -1;
      if (now >= deadline) return 0;
      continue;
    }
    if (information.si_pid == child) {
      if (monotonic_milliseconds(&now) != 0) return -1;
      return now < deadline ? 1 : 0;
    }
    if (monotonic_milliseconds(&now) != 0) return -1;
    if (now >= deadline) return 0;
    remaining = deadline - now;
    sleep_ms = remaining < FORMATTER_WAIT_POLL_MS ? remaining
                                                  : FORMATTER_WAIT_POLL_MS;
    delay.tv_sec = (time_t)(sleep_ms / UINT64_C(1000));
    delay.tv_nsec = (long)((sleep_ms % UINT64_C(1000)) * UINT64_C(1000000));
    while (nanosleep(&delay, &delay) != 0) {
      if (errno != EINTR || monotonic_milliseconds(&now) != 0) return -1;
      if (now >= deadline) return 0;
    }
  }
}

static int establish_formatter_process_group(pid_t child) {
  pid_t group;
  if (setpgid(child, child) == 0) return 0;
  if (errno != EACCES && errno != EPERM) return -1;
  group = getpgid(child);
  return group == child ? 0 : -1;
}

#if defined(__linux__) && defined(PR_SET_CHILD_SUBREAPER)
static int configure_formatter_subreaper(void) {
  return prctl(PR_SET_CHILD_SUBREAPER, 1);
}
#endif

#if defined(__linux__)
static int configure_formatter_parent_death(pid_t expected_parent) {
  if (prctl(PR_SET_PDEATHSIG, SIGKILL) != 0) return -1;
  if (getppid() != expected_parent) {
    errno = ESRCH;
    return -1;
  }
  return 0;
}
#endif

static int signal_unreaped_formatter_group(pid_t leader, int signal_number) {
  /* The unreaped leader reserves both its PID and PGID against reuse. */
  if (kill(-leader, signal_number) == 0 || errno == ESRCH) return 0;
  return -1;
}

static int formatter_group_quiescent(pid_t leader) {
#if defined(__linux__)
  if (kill(-leader, 0) == 0) return 0;
  return errno == ESRCH ? 1 : -1;
#else
  if (getpgid(leader) >= 0) return 0;
  return errno == ESRCH ? 1 : -1;
#endif
}

#if defined(PORTABLE_CODEX_HAS_EXT4_INSPECTION_ABI) &&                     \
    !defined(PORTABLE_CODEX_FORMATTER_SUPERVISOR_TEST)
static int reap_formatter_leader(pid_t leader, int *wait_status) {
  pid_t waited;
  do {
    waited = waitpid(leader, wait_status, WNOHANG);
  } while (waited < 0 && errno == EINTR);
  return waited == leader ? 0 : -1;
}
#endif

static int reap_formatter_group_until(pid_t leader, uint64_t deadline,
                                      int *leader_wait_status) {
  int discarded_descendant_status;
  int reaped_leader_status;
  pid_t waited;

  /* The caller must first observe this exact leader with waitid(WNOWAIT).
   * That retained zombie is the PID/PGID identity fence for the last signal. */
  for (;;) {
    waited = waitpid(leader, &reaped_leader_status, WNOHANG);
    if (waited == leader) break;
    if (waited < 0 && errno == EINTR) {
      uint64_t now;
      if (monotonic_milliseconds(&now) != 0 || now >= deadline) return -1;
      continue;
    }
    return -1;
  }
  *leader_wait_status = reaped_leader_status;

  for (;;) {
    struct timespec delay;
    uint64_t now;
    uint64_t remaining;
    uint64_t sleep_ms;
    int no_group_children = 0;
    int quiescent;

    waited = waitpid(-leader, &discarded_descendant_status, WNOHANG);
    if (waited > 0) {
      if (monotonic_milliseconds(&now) != 0 || now >= deadline) return -1;
      continue;
    }
    if (waited < 0) {
      if (errno == EINTR) {
        if (monotonic_milliseconds(&now) != 0 || now >= deadline) return -1;
        continue;
      }
      if (errno != ECHILD) return -1;
      no_group_children = 1;
    }

    /* Signal zero only probes existence; no deliverable group signal follows
     * the leader reap, so a reused numeric PGID cannot be killed here. */
    quiescent = formatter_group_quiescent(leader);
    if (quiescent < 0) return -1;
    if (quiescent > 0) {
      if (!no_group_children || monotonic_milliseconds(&now) != 0 ||
          now >= deadline) {
        return -1;
      }
      return 0;
    }
    if (monotonic_milliseconds(&now) != 0 || now >= deadline) return -1;
    remaining = deadline - now;
    sleep_ms = remaining < FORMATTER_WAIT_POLL_MS ? remaining
                                                  : FORMATTER_WAIT_POLL_MS;
    delay.tv_sec = (time_t)(sleep_ms / UINT64_C(1000));
    delay.tv_nsec =
        (long)((sleep_ms % UINT64_C(1000)) * UINT64_C(1000000));
    while (nanosleep(&delay, &delay) != 0) {
      if (errno != EINTR || monotonic_milliseconds(&now) != 0 ||
          now >= deadline) {
        return -1;
      }
    }
  }
}

static int finish_formatter_group(pid_t leader, uint64_t deadline,
                                  int *wait_status) {
  if (signal_unreaped_formatter_group(leader, SIGKILL) != 0) return -1;
  return reap_formatter_group_until(leader, deadline, wait_status);
}

static void force_formatter_group_cleanup(pid_t leader,
                                          uint64_t hard_deadline,
                                          int *wait_status) {
  if (signal_unreaped_formatter_group(leader, SIGKILL) != 0 ||
      formatter_child_terminal_until(leader, hard_deadline) <= 0) {
    return;
  }
  (void)finish_formatter_group(leader, hard_deadline, wait_status);
}

static int supervise_formatter(pid_t leader, uint64_t hard_deadline,
                               uint64_t execution_timeout_ms,
                               uint64_t term_grace_ms,
                               uint64_t kill_reap_timeout_ms,
                               int *wait_status) {
  uint64_t deadline;
  uint64_t termination_reserve;
  int wait_result;

  if (term_grace_ms > UINT64_MAX - kill_reap_timeout_ms) {
    force_formatter_group_cleanup(leader, hard_deadline, wait_status);
    return FORMATTER_SUPERVISION_ERROR;
  }
  termination_reserve = term_grace_ms + kill_reap_timeout_ms;
  if (phase_deadline(hard_deadline, execution_timeout_ms,
                     termination_reserve, &deadline) != 0) {
    force_formatter_group_cleanup(leader, hard_deadline, wait_status);
    return FORMATTER_SUPERVISION_ERROR;
  }
  wait_result = formatter_child_terminal_until(leader, deadline);
  if (wait_result > 0) {
    return finish_formatter_group(leader, hard_deadline, wait_status) == 0
               ? FORMATTER_SUPERVISION_REAPED
               : FORMATTER_SUPERVISION_ERROR;
  }
  if (wait_result < 0 ||
      signal_unreaped_formatter_group(leader, SIGTERM) != 0) {
    force_formatter_group_cleanup(leader, hard_deadline, wait_status);
    return FORMATTER_SUPERVISION_ERROR;
  }

  if (phase_deadline(hard_deadline, term_grace_ms, kill_reap_timeout_ms,
                     &deadline) != 0) {
    force_formatter_group_cleanup(leader, hard_deadline, wait_status);
    return FORMATTER_SUPERVISION_ERROR;
  }
  wait_result = formatter_child_terminal_until(leader, deadline);
  if (wait_result > 0) {
    return finish_formatter_group(leader, hard_deadline, wait_status) == 0
               ? FORMATTER_SUPERVISION_TIMED_OUT
               : FORMATTER_SUPERVISION_ERROR;
  }
  if (wait_result < 0 ||
      signal_unreaped_formatter_group(leader, SIGKILL) != 0) {
    force_formatter_group_cleanup(leader, hard_deadline, wait_status);
    return FORMATTER_SUPERVISION_ERROR;
  }

  wait_result = formatter_child_terminal_until(leader, hard_deadline);
  if (wait_result <= 0) {
    force_formatter_group_cleanup(leader, hard_deadline, wait_status);
    return FORMATTER_SUPERVISION_ERROR;
  }
  return finish_formatter_group(leader, hard_deadline, wait_status) == 0
             ? FORMATTER_SUPERVISION_TIMED_OUT
             : FORMATTER_SUPERVISION_ERROR;
}

#endif

#if !defined(PORTABLE_CODEX_HAS_EXT4_INSPECTION_ABI) &&                     \
    !defined(PORTABLE_CODEX_FORMATTER_SUPERVISOR_TEST)

int main(int argc, char **argv) {
  (void)argc;
  (void)argv;
  return INSPECTOR_EXIT_UNSUPPORTED;
}

#elif defined(PORTABLE_CODEX_HAS_EXT4_INSPECTION_ABI) &&                    \
    !defined(PORTABLE_CODEX_FORMATTER_SUPERVISOR_TEST)

#define FILE_HANDLE_LIMIT_BYTES 128U
#define FILESYSTEM_UUID_BYTES 16U
#define SHA256_BLOCK_BYTES 64U
#define SHA256_DIGEST_BYTES 32U

struct sha256_context {
  uint8_t block[SHA256_BLOCK_BYTES];
  uint32_t block_length;
  uint64_t total_bits;
  uint32_t state[8];
};

static const uint32_t sha256_round_constants[64] = {
    0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U, 0x3956c25bU,
    0x59f111f1U, 0x923f82a4U, 0xab1c5ed5U, 0xd807aa98U, 0x12835b01U,
    0x243185beU, 0x550c7dc3U, 0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U,
    0xc19bf174U, 0xe49b69c1U, 0xefbe4786U, 0x0fc19dc6U, 0x240ca1ccU,
    0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU, 0x983e5152U,
    0xa831c66dU, 0xb00327c8U, 0xbf597fc7U, 0xc6e00bf3U, 0xd5a79147U,
    0x06ca6351U, 0x14292967U, 0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU,
    0x53380d13U, 0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U,
    0xa2bfe8a1U, 0xa81a664bU, 0xc24b8b70U, 0xc76c51a3U, 0xd192e819U,
    0xd6990624U, 0xf40e3585U, 0x106aa070U, 0x19a4c116U, 0x1e376c08U,
    0x2748774cU, 0x34b0bcb5U, 0x391c0cb3U, 0x4ed8aa4aU, 0x5b9cca4fU,
    0x682e6ff3U, 0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U,
    0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U,
};

static uint32_t rotate_right(uint32_t value, uint32_t count) {
  return (value >> count) | (value << (32U - count));
}

static void sha256_transform(struct sha256_context *context,
                             const uint8_t block[SHA256_BLOCK_BYTES]) {
  uint32_t words[64];
  uint32_t working[8];
  uint32_t index;

  for (index = 0; index < 16U; index += 1U) {
    const uint32_t offset = index * 4U;
    words[index] = ((uint32_t)block[offset] << 24U) |
                   ((uint32_t)block[offset + 1U] << 16U) |
                   ((uint32_t)block[offset + 2U] << 8U) |
                   (uint32_t)block[offset + 3U];
  }
  for (index = 16U; index < 64U; index += 1U) {
    const uint32_t s0 = rotate_right(words[index - 15U], 7U) ^
                        rotate_right(words[index - 15U], 18U) ^
                        (words[index - 15U] >> 3U);
    const uint32_t s1 = rotate_right(words[index - 2U], 17U) ^
                        rotate_right(words[index - 2U], 19U) ^
                        (words[index - 2U] >> 10U);
    words[index] = words[index - 16U] + s0 + words[index - 7U] + s1;
  }

  for (index = 0; index < 8U; index += 1U) {
    working[index] = context->state[index];
  }
  for (index = 0; index < 64U; index += 1U) {
    const uint32_t choose =
        (working[4] & working[5]) ^ ((~working[4]) & working[6]);
    const uint32_t majority = (working[0] & working[1]) ^
                              (working[0] & working[2]) ^
                              (working[1] & working[2]);
    const uint32_t sigma0 = rotate_right(working[0], 2U) ^
                            rotate_right(working[0], 13U) ^
                            rotate_right(working[0], 22U);
    const uint32_t sigma1 = rotate_right(working[4], 6U) ^
                            rotate_right(working[4], 11U) ^
                            rotate_right(working[4], 25U);
    const uint32_t first = working[7] + sigma1 + choose +
                           sha256_round_constants[index] + words[index];
    const uint32_t second = sigma0 + majority;

    working[7] = working[6];
    working[6] = working[5];
    working[5] = working[4];
    working[4] = working[3] + first;
    working[3] = working[2];
    working[2] = working[1];
    working[1] = working[0];
    working[0] = first + second;
  }
  for (index = 0; index < 8U; index += 1U) {
    context->state[index] += working[index];
  }
}

static void sha256_initialize(struct sha256_context *context) {
  memset(context, 0, sizeof(*context));
  context->state[0] = 0x6a09e667U;
  context->state[1] = 0xbb67ae85U;
  context->state[2] = 0x3c6ef372U;
  context->state[3] = 0xa54ff53aU;
  context->state[4] = 0x510e527fU;
  context->state[5] = 0x9b05688cU;
  context->state[6] = 0x1f83d9abU;
  context->state[7] = 0x5be0cd19U;
}

static void sha256_update(struct sha256_context *context, const uint8_t *input,
                          size_t length) {
  size_t index;
  for (index = 0; index < length; index += 1U) {
    context->block[context->block_length] = input[index];
    context->block_length += 1U;
    if (context->block_length == SHA256_BLOCK_BYTES) {
      sha256_transform(context, context->block);
      context->total_bits += SHA256_BLOCK_BYTES * 8U;
      context->block_length = 0;
    }
  }
}

static void sha256_finalize(struct sha256_context *context,
                            uint8_t digest[SHA256_DIGEST_BYTES]) {
  uint32_t index = context->block_length;
  uint64_t total_bits;

  context->block[index] = 0x80U;
  index += 1U;
  if (index > 56U) {
    while (index < SHA256_BLOCK_BYTES) {
      context->block[index] = 0;
      index += 1U;
    }
    sha256_transform(context, context->block);
    index = 0;
  }
  while (index < 56U) {
    context->block[index] = 0;
    index += 1U;
  }

  total_bits = context->total_bits + ((uint64_t)context->block_length * 8U);
  for (index = 0; index < 8U; index += 1U) {
    context->block[63U - index] = (uint8_t)(total_bits >> (index * 8U));
  }
  sha256_transform(context, context->block);

  for (index = 0; index < 8U; index += 1U) {
    digest[index * 4U] = (uint8_t)(context->state[index] >> 24U);
    digest[index * 4U + 1U] = (uint8_t)(context->state[index] >> 16U);
    digest[index * 4U + 2U] = (uint8_t)(context->state[index] >> 8U);
    digest[index * 4U + 3U] = (uint8_t)context->state[index];
  }
}

static int classify_path_errno(int error_number) {
  switch (error_number) {
  case ENOENT:
  case ENOTDIR:
    return INSPECTOR_EXIT_MISSING;
  case EACCES:
  case EPERM:
    return INSPECTOR_EXIT_UNREADABLE;
  case EEXIST:
    return INSPECTOR_EXIT_EXISTS;
  case EXDEV:
  case ELOOP:
  case ESTALE:
  case ENAMETOOLONG:
    return INSPECTOR_EXIT_MISMATCH;
  case ENOSYS:
  case EINVAL:
  case ENODEV:
  case ENOTTY:
  case EOPNOTSUPP:
  case EOVERFLOW:
    return INSPECTOR_EXIT_UNSUPPORTED;
  default:
    return INSPECTOR_EXIT_IO;
  }
}

static int call_openat2(int directory_fd, const char *path,
                        const struct open_how *how) {
#if defined(SYS_openat2)
  return (int)syscall(SYS_openat2, directory_fd, path, how, sizeof(*how));
#else
  return (int)syscall(__NR_openat2, directory_fd, path, how, sizeof(*how));
#endif
}

#define LOOP_SCAN_LIMIT 4096U
#define LOOP_ATTACH_ATTEMPTS 256U
#define LOOP_SETTLE_ATTEMPTS 100U
#define LOOP_BLOCK_SIZE 512U
#define CONTROL_FILE_MODE 0600
#define DIRECTORY_MODE 0700
#define IMAGE_MODE 0600
#define MOUNTINFO_BYTES_LIMIT (1024U * 1024U)

static int valid_direct_name(const char *name) {
  const unsigned char *cursor = (const unsigned char *)name;
  size_t length = 0U;
  if (name[0] == '\0' || strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
    return 0;
  }
  while (*cursor != '\0') {
    if (*cursor == '/' || *cursor < 0x20U || *cursor == 0x7fU) return 0;
    cursor += 1;
    length += 1U;
    if (length > 255U) return 0;
  }
  return 1;
}

static int valid_absolute_executable(const char *path) {
  const unsigned char *cursor = (const unsigned char *)path;
  size_t length = 0U;
  if (path[0] != '/') return 0;
  while (*cursor != '\0') {
    if (*cursor < 0x20U || *cursor == 0x7fU) return 0;
    cursor += 1;
    length += 1U;
    if (length > 4095U) return 0;
  }
  return length > 1U;
}

static int parse_u64_decimal(const char *value, uint64_t *output) {
  uint64_t parsed = 0U;
  const unsigned char *cursor = (const unsigned char *)value;
  if (*cursor == '\0') return 0;
  if (*cursor == '0' && cursor[1] != '\0') return 0;
  while (*cursor != '\0') {
    uint64_t digit;
    if (*cursor < '0' || *cursor > '9') return 0;
    digit = (uint64_t)(*cursor - '0');
    if (parsed > (UINT64_MAX - digit) / 10U) return 0;
    parsed = parsed * 10U + digit;
    cursor += 1;
  }
  *output = parsed;
  return 1;
}

static int open_root_directory(const char *root_path) {
  struct open_how how;
  memset(&how, 0, sizeof(how));
  how.flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
  how.resolve = RESOLVE_NO_MAGICLINKS | RESOLVE_NO_SYMLINKS;
  return call_openat2(AT_FDCWD, root_path, &how);
}

static int open_relative_directory(int root_fd, const char *relative_path) {
  struct open_how how;
  memset(&how, 0, sizeof(how));
  how.flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
  how.resolve = RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS |
                RESOLVE_NO_SYMLINKS | RESOLVE_NO_XDEV;
  return call_openat2(root_fd, relative_path, &how);
}

static int open_direct_child(int parent_fd, const char *name, uint64_t flags,
                             int allow_mount_transition) {
  struct open_how how;
  if (!valid_direct_name(name)) {
    errno = EINVAL;
    return -1;
  }
  memset(&how, 0, sizeof(how));
  how.flags = flags | O_NOFOLLOW | O_CLOEXEC;
  how.resolve = RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS |
                RESOLVE_NO_SYMLINKS;
  if (!allow_mount_transition) how.resolve |= RESOLVE_NO_XDEV;
  return call_openat2(parent_fd, name, &how);
}

static int extended_acl_state(int fd) {
  static const char *const names[] = {"system.posix_acl_access",
                                      "system.posix_acl_default"};
  size_t index;
  for (index = 0U; index < sizeof(names) / sizeof(names[0]); index += 1U) {
    errno = 0;
    if (fgetxattr(fd, names[index], NULL, 0U) >= 0) return 1;
    if (errno == ENODATA) continue;
#if defined(ENOATTR) && ENOATTR != ENODATA
    if (errno == ENOATTR) continue;
#endif
    if (errno == ENOTSUP || errno == EOPNOTSUPP) continue;
    return -1;
  }
  return 0;
}

static int private_policy_status(int fd, mode_t type, mode_t mode,
                                 int single_link) {
  struct stat metadata;
  int acl_state;
  if (fstat(fd, &metadata) != 0) return -1;
  if ((metadata.st_mode & S_IFMT) != type || metadata.st_uid != getuid() ||
      (metadata.st_mode & 0777U) != mode || metadata.st_nlink < 1 ||
      (single_link && metadata.st_nlink != 1)) {
    return 0;
  }
  acl_state = extended_acl_state(fd);
  if (acl_state < 0) return -1;
  return acl_state == 0 ? 1 : 0;
}

static int unlinked_private_policy_status(int fd, mode_t type, mode_t mode) {
  struct stat metadata;
  int acl_state;
  if (fstat(fd, &metadata) != 0) return -1;
  if ((metadata.st_mode & S_IFMT) != type || metadata.st_uid != getuid() ||
      (metadata.st_mode & 0777U) != mode || metadata.st_nlink != 0) {
    return 0;
  }
  acl_state = extended_acl_state(fd);
  if (acl_state < 0) return -1;
  return acl_state == 0 ? 1 : 0;
}

static int require_private_policy(int fd, mode_t type, mode_t mode,
                                  int single_link, int dispatched) {
  const int state = private_policy_status(fd, type, mode, single_link);
  if (state > 0) return EXIT_SUCCESS;
  if (state == 0 && !dispatched) return INSPECTOR_EXIT_MISMATCH;
  return dispatched ? INSPECTOR_EXIT_OUTCOME_UNCERTAIN : INSPECTOR_EXIT_IO;
}

static int require_fd_identity(int fd, const char *device_text,
                               const char *inode_text, int dispatched) {
  struct stat metadata;
  uint64_t expected_device;
  uint64_t expected_inode;
  if (!parse_u64_decimal(device_text, &expected_device) ||
      !parse_u64_decimal(inode_text, &expected_inode) || expected_inode == 0U) {
    return INSPECTOR_EXIT_USAGE;
  }
  if (fstat(fd, &metadata) != 0) {
    return dispatched ? INSPECTOR_EXIT_OUTCOME_UNCERTAIN : INSPECTOR_EXIT_IO;
  }
  if ((uint64_t)metadata.st_dev != expected_device ||
      (uint64_t)metadata.st_ino != expected_inode) {
    return dispatched ? INSPECTOR_EXIT_OUTCOME_UNCERTAIN
                      : INSPECTOR_EXIT_MISMATCH;
  }
  return EXIT_SUCCESS;
}

static int format_proc_fd_path(int fd, char output[64]) {
  const int length = snprintf(output, 64U, "/proc/self/fd/%d", fd);
  return length > 0 && length < 64;
}

static int format_proc_fd_child_path(int parent_fd, const char *name,
                                     char output[320]) {
  const int length = snprintf(output, 320U, "/proc/self/fd/%d/%s", parent_fd,
                              name);
  return valid_direct_name(name) && length > 0 && length < 320;
}

static int clear_close_on_exec(int fd) {
  const int flags = fcntl(fd, F_GETFD);
  if (flags < 0) return -1;
  return fcntl(fd, F_SETFD, flags & ~FD_CLOEXEC);
}

static int close_helper_authorities_for_exec(int retained_fd) {
  if (retained_fd <= STDERR_FILENO) {
    errno = EBADF;
    return -1;
  }
  if (retained_fd != 3 && dup3(retained_fd, 3, 0) < 0) return -1;
  if (clear_close_on_exec(3) != 0) return -1;
#if defined(SYS_close_range)
  return (int)syscall(SYS_close_range, 4U, UINT_MAX, CLOSE_RANGE_UNSHARE);
#else
  return (int)syscall(__NR_close_range, 4U, UINT_MAX, CLOSE_RANGE_UNSHARE);
#endif
}

static int open_verified_dev_null(void) {
  struct stat metadata;
  int null_fd = open("/dev/null", O_RDWR | O_NOFOLLOW | O_CLOEXEC);
  if (null_fd < 0) return -1;
  if (fstat(null_fd, &metadata) != 0 || !S_ISCHR(metadata.st_mode) ||
      major(metadata.st_rdev) != 1U || minor(metadata.st_rdev) != 3U) {
    (void)close(null_fd);
    errno = ENODEV;
    return -1;
  }
  return null_fd;
}

static int redirect_formatter_standard_descriptors(int null_fd) {
  int descriptor;
  for (descriptor = STDIN_FILENO; descriptor <= STDERR_FILENO;
       descriptor += 1) {
    if (dup3(null_fd, descriptor, 0) < 0) return -1;
  }
  return 0;
}

static void terminate_unestablished_formatter(pid_t child,
                                               uint64_t hard_deadline) {
  int wait_status;
  if (kill(child, SIGKILL) != 0 && errno != ESRCH) return;
  if (formatter_child_terminal_until(child, hard_deadline) > 0) {
    (void)reap_formatter_leader(child, &wait_status);
  }
}

static int standard_descriptors_present(void) {
  int descriptor;
  for (descriptor = STDIN_FILENO; descriptor <= STDERR_FILENO;
       descriptor += 1) {
    if (fcntl(descriptor, F_GETFD) < 0) return 0;
  }
  return 1;
}

static int run_mkfs_on_fd(const char *executable, int image_fd,
                          uint64_t hard_deadline) {
  static char *const environment[] = {(char *)"LANG=C", (char *)"LC_ALL=C",
                                      NULL};
  char proc_path[64];
  char root_owner[96];
  pid_t child;
  pid_t expected_parent;
  int null_fd = -1;
  int supervision;
  int wait_status = 0;
  int root_owner_length;
  uint64_t launch_deadline;
  root_owner_length = snprintf(root_owner, sizeof(root_owner),
                               "root_owner=%ju:%ju", (uintmax_t)getuid(),
                               (uintmax_t)getgid());
  if (!valid_absolute_executable(executable) ||
      !format_proc_fd_path(3, proc_path) || root_owner_length <= 0 ||
      (size_t)root_owner_length >= sizeof(root_owner)) {
    return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  }
  null_fd = open_verified_dev_null();
  if (null_fd < 0) return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  if (phase_deadline(hard_deadline, FORMATTER_EXECUTION_TIMEOUT_MS,
                     FORMATTER_TERM_GRACE_MS +
                         FORMATTER_KILL_REAP_TIMEOUT_MS,
                     &launch_deadline) != 0) {
    (void)close(null_fd);
    return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  }
  (void)launch_deadline;
  if (configure_formatter_subreaper() != 0) {
    (void)close(null_fd);
    return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  }
  expected_parent = getpid();
  child = fork();
  if (child < 0) {
    (void)close(null_fd);
    return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  }
  if (child == 0) {
    char *const arguments[] = {(char *)executable, (char *)"-F", (char *)"-q",
                               (char *)"-E", root_owner, (char *)"--",
                               proc_path, NULL};
    /*
     * Group supervision requires the reviewed formatter tree to remain in this
     * PGID. If the outer timeout kills this helper first, PDEATHSIG covers only
     * this direct leader, so the configured formatter must not leave long-lived
     * descendants, even in the same PGID.
     */
    if (configure_formatter_parent_death(expected_parent) != 0 ||
        setpgid(0, 0) != 0 ||
        redirect_formatter_standard_descriptors(null_fd) != 0 ||
        close_helper_authorities_for_exec(image_fd) != 0) {
      _exit(126);
    }
    execve(executable, arguments, environment);
    _exit(127);
  }
  if (establish_formatter_process_group(child) != 0) {
    (void)close(null_fd);
    terminate_unestablished_formatter(child, hard_deadline);
    return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  }
  (void)close(null_fd);
  supervision = supervise_formatter(
      child, hard_deadline, FORMATTER_EXECUTION_TIMEOUT_MS,
      FORMATTER_TERM_GRACE_MS, FORMATTER_KILL_REAP_TIMEOUT_MS, &wait_status);
  /* This proves stability only for the trusted tree that remains in its PGID. */
  if (supervision != FORMATTER_SUPERVISION_REAPED ||
      !WIFEXITED(wait_status) ||
      WEXITSTATUS(wait_status) != 0) {
    return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  }
  return EXIT_SUCCESS;
}

static int print_ok(void) {
  if (printf("{\"status\":\"ok\"}\n") < 0 || fflush(stdout) != 0) {
    return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  }
  return EXIT_SUCCESS;
}

static int open_operation_parent(const char *root_path,
                                 const char *relative_path, int *root_fd,
                                 int *parent_fd) {
  int status;
  *root_fd = open_root_directory(root_path);
  if (*root_fd < 0) return classify_path_errno(errno);
  *parent_fd = open_relative_directory(*root_fd, relative_path);
  if (*parent_fd < 0) return classify_path_errno(errno);
  status = require_private_policy(*parent_fd, S_IFDIR, DIRECTORY_MODE, 0, 0);
  return status;
}

static void format_uuid(const uint8_t uuid[16], char output[37]) {
  static const char hexadecimal[] = "0123456789abcdef";
  size_t input_index;
  size_t output_index = 0;
  for (input_index = 0; input_index < 16U; input_index += 1U) {
    if (input_index == 4U || input_index == 6U || input_index == 8U ||
        input_index == 10U) {
      output[output_index] = '-';
      output_index += 1U;
    }
    output[output_index] = hexadecimal[uuid[input_index] >> 4U];
    output[output_index + 1U] = hexadecimal[uuid[input_index] & 0x0fU];
    output_index += 2U;
  }
  output[output_index] = '\0';
}

static int read_filesystem_uuid(int root_fd,
                                uint8_t output[FILESYSTEM_UUID_BYTES]) {
#if defined(EXT4_IOC_GETFSUUID)
  struct fsuuid *ext4_uuid;
  int ext4_error;

  /*
   * EXT4_IOC_GETFSUUID predates the generic filesystem UUID ioctl and is
   * required on ext4 kernels such as Linux 6.8. The flexible-array UAPI
   * copies back both this fixed header and exactly fsu_len UUID bytes.
   */
  ext4_uuid = calloc(1U, sizeof(*ext4_uuid) + FILESYSTEM_UUID_BYTES);
  if (ext4_uuid == NULL) return INSPECTOR_EXIT_IO;
  ext4_uuid->fsu_len = FILESYSTEM_UUID_BYTES;
  ext4_uuid->fsu_flags = 0U;
  if (ioctl(root_fd, EXT4_IOC_GETFSUUID, ext4_uuid) == 0) {
    if (ext4_uuid->fsu_len != FILESYSTEM_UUID_BYTES ||
        ext4_uuid->fsu_flags != 0U) {
      free(ext4_uuid);
      return INSPECTOR_EXIT_UNSUPPORTED;
    }
    memcpy(output, ext4_uuid->fsu_uuid, FILESYSTEM_UUID_BYTES);
    free(ext4_uuid);
    return EXIT_SUCCESS;
  }
  ext4_error = errno;
  free(ext4_uuid);
#if defined(FS_IOC_GETFSUUID)
  if (classify_path_errno(ext4_error) != INSPECTOR_EXIT_UNSUPPORTED) {
    return classify_path_errno(ext4_error);
  }
#else
  return classify_path_errno(ext4_error);
#endif
#endif

#if defined(FS_IOC_GETFSUUID)
  {
    struct fsuuid2 generic_uuid;

    /* New kernels expose the same external UUID through the generic UAPI. */
    memset(&generic_uuid, 0, sizeof(generic_uuid));
    if (ioctl(root_fd, FS_IOC_GETFSUUID, &generic_uuid) != 0) {
      return classify_path_errno(errno);
    }
    if (generic_uuid.len != FILESYSTEM_UUID_BYTES) {
      return INSPECTOR_EXIT_UNSUPPORTED;
    }
    memcpy(output, generic_uuid.uuid, FILESYSTEM_UUID_BYTES);
    return EXIT_SUCCESS;
  }
#else
  return INSPECTOR_EXIT_UNSUPPORTED;
#endif
}

static void build_object_id(const uint8_t filesystem_uuid[16],
                            const struct file_handle *handle,
                            char output[73]) {
  static const uint8_t domain[] =
      "linux-ext4-file-handle-sha256-v1";
  static const char hexadecimal[] = "0123456789abcdef";
  struct sha256_context context;
  uint8_t digest[SHA256_DIGEST_BYTES];
  uint8_t encoded_type[4];
  uint32_t handle_type = (uint32_t)handle->handle_type;
  size_t index;

  encoded_type[0] = (uint8_t)(handle_type >> 24U);
  encoded_type[1] = (uint8_t)(handle_type >> 16U);
  encoded_type[2] = (uint8_t)(handle_type >> 8U);
  encoded_type[3] = (uint8_t)handle_type;
  sha256_initialize(&context);
  /* SHA256(domain || NUL || uuid[16] || be32(handle_type) || handle bytes). */
  sha256_update(&context, domain, sizeof(domain));
  sha256_update(&context, filesystem_uuid, 16U);
  sha256_update(&context, encoded_type, sizeof(encoded_type));
  sha256_update(&context, handle->f_handle, handle->handle_bytes);
  sha256_finalize(&context, digest);

  memcpy(output, "ext4fh1:", 8U);
  for (index = 0; index < SHA256_DIGEST_BYTES; index += 1U) {
    output[8U + index * 2U] = hexadecimal[digest[index] >> 4U];
    output[8U + index * 2U + 1U] = hexadecimal[digest[index] & 0x0fU];
  }
  output[72] = '\0';
}

static int read_persistent_identity_from_fd(int fd, char uuid_text[37],
                                            char object_id[73]) {
  uint8_t filesystem_uuid[FILESYSTEM_UUID_BYTES];
  struct statfs filesystem;
  struct file_handle *handle = NULL;
  int mount_id = 0;
  int status;
  if (fstatfs(fd, &filesystem) != 0) return classify_path_errno(errno);
  if ((unsigned long)filesystem.f_type != (unsigned long)EXT4_SUPER_MAGIC) {
    return INSPECTOR_EXIT_UNSUPPORTED;
  }
  status = read_filesystem_uuid(fd, filesystem_uuid);
  if (status != EXIT_SUCCESS) return status;
  handle = calloc(1U, sizeof(*handle) + FILE_HANDLE_LIMIT_BYTES);
  if (handle == NULL) return INSPECTOR_EXIT_IO;
  handle->handle_bytes = FILE_HANDLE_LIMIT_BYTES;
  if (name_to_handle_at(fd, "", handle, &mount_id, AT_EMPTY_PATH) != 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  if (handle->handle_bytes == 0U ||
      handle->handle_bytes > FILE_HANDLE_LIMIT_BYTES ||
      handle->handle_type <= 0) {
    status = INSPECTOR_EXIT_UNSUPPORTED;
    goto cleanup;
  }
  format_uuid(filesystem_uuid, uuid_text);
  build_object_id(filesystem_uuid, handle, object_id);
  status = EXIT_SUCCESS;

cleanup:
  free(handle);
  return status;
}

static int require_persistent_identity(int fd, const char *filesystem_id,
                                       const char *object_id,
                                       int dispatched) {
  char actual_uuid[37];
  char actual_object_id[73];
  int status;
  if (strlen(filesystem_id) != 43U ||
      strncmp(filesystem_id, "ext4fs:", 7U) != 0 ||
      strlen(object_id) != 72U || strncmp(object_id, "ext4fh1:", 8U) != 0) {
    return INSPECTOR_EXIT_USAGE;
  }
  status = read_persistent_identity_from_fd(fd, actual_uuid,
                                            actual_object_id);
  if (status != EXIT_SUCCESS) {
    return dispatched ? INSPECTOR_EXIT_OUTCOME_UNCERTAIN : status;
  }
  if (strcmp(filesystem_id + 7U, actual_uuid) != 0 ||
      strcmp(object_id, actual_object_id) != 0) {
    return dispatched ? INSPECTOR_EXIT_OUTCOME_UNCERTAIN
                      : INSPECTOR_EXIT_MISMATCH;
  }
  return EXIT_SUCCESS;
}

static int inspect_path(const char *root_path, const char *relative_path) {
  uint8_t filesystem_uuid[FILESYSTEM_UUID_BYTES];
  struct stat root_metadata;
  struct stat target_metadata;
  struct statfs root_filesystem;
  struct open_how how;
  struct open_how root_how;
  struct file_handle *handle = NULL;
  char object_id[73];
  char uuid_text[37];
  int mount_id = 0;
  int root_fd = -1;
  int target_fd = -1;
  int status = INSPECTOR_EXIT_IO;

  if (root_path[0] != '/' || relative_path[0] == '\0' ||
      relative_path[0] == '/') {
    return INSPECTOR_EXIT_USAGE;
  }

  memset(&root_how, 0, sizeof(root_how));
  root_how.flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
  root_how.resolve = RESOLVE_NO_MAGICLINKS | RESOLVE_NO_SYMLINKS;
  root_fd = call_openat2(AT_FDCWD, root_path, &root_how);
  if (root_fd < 0) return classify_path_errno(errno);
  if (fstat(root_fd, &root_metadata) != 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  if (!S_ISDIR(root_metadata.st_mode)) {
    status = INSPECTOR_EXIT_MISMATCH;
    goto cleanup;
  }
  if (fstatfs(root_fd, &root_filesystem) != 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  if ((unsigned long)root_filesystem.f_type !=
      (unsigned long)EXT4_SUPER_MAGIC) {
    status = INSPECTOR_EXIT_UNSUPPORTED;
    goto cleanup;
  }

  status = read_filesystem_uuid(root_fd, filesystem_uuid);
  if (status != EXIT_SUCCESS) goto cleanup;

  memset(&how, 0, sizeof(how));
  how.flags = O_PATH | O_NOFOLLOW | O_CLOEXEC;
  how.resolve = RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS |
                RESOLVE_NO_SYMLINKS | RESOLVE_NO_XDEV;
  target_fd = call_openat2(root_fd, relative_path, &how);
  if (target_fd < 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  if (fstat(target_fd, &target_metadata) != 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  if (target_metadata.st_dev != root_metadata.st_dev) {
    status = INSPECTOR_EXIT_MISMATCH;
    goto cleanup;
  }

  handle = calloc(1U, sizeof(*handle) + FILE_HANDLE_LIMIT_BYTES);
  if (handle == NULL) goto cleanup;
  handle->handle_bytes = FILE_HANDLE_LIMIT_BYTES;
  if (name_to_handle_at(target_fd, "", handle, &mount_id, AT_EMPTY_PATH) != 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  if (handle->handle_bytes == 0U ||
      handle->handle_bytes > FILE_HANDLE_LIMIT_BYTES ||
      handle->handle_type <= 0) {
    status = INSPECTOR_EXIT_UNSUPPORTED;
    goto cleanup;
  }

  format_uuid(filesystem_uuid, uuid_text);
  build_object_id(filesystem_uuid, handle, object_id);
  if (printf("{\"filesystemUuid\":\"%s\",\"device\":\"%" PRIuMAX
             "\",\"inode\":\"%" PRIuMAX "\",\"objectId\":\"%s\"}\n",
             uuid_text, (uintmax_t)target_metadata.st_dev,
             (uintmax_t)target_metadata.st_ino, object_id) < 0 ||
      fflush(stdout) != 0) {
    goto cleanup;
  }
  status = EXIT_SUCCESS;

cleanup:
  free(handle);
  if (target_fd >= 0) (void)close(target_fd);
  if (root_fd >= 0) (void)close(root_fd);
  return status;
}

static int create_image_file(const char *root_path, const char *relative_parent,
                             const char *name, const char *size_text,
                             const char *parent_device,
                             const char *parent_inode) {
  uint64_t size;
  struct stat metadata;
  int root_fd = -1;
  int parent_fd = -1;
  int image_fd = -1;
  int created = 0;
  int status;
  if (!valid_direct_name(name) || !parse_u64_decimal(size_text, &size) ||
      size < 1024U * 1024U || size > (UINT64_C(8) << 40) ||
      size > (uint64_t)INT64_MAX || size % LOOP_BLOCK_SIZE != 0U) {
    return INSPECTOR_EXIT_USAGE;
  }
  status = open_operation_parent(root_path, relative_parent, &root_fd,
                                 &parent_fd);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_fd_identity(parent_fd, parent_device, parent_inode, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  image_fd = openat(parent_fd, name,
                    O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
                    IMAGE_MODE);
  if (image_fd < 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  created = 1;
  if (fchmod(image_fd, IMAGE_MODE) != 0 ||
      ftruncate(image_fd, (off_t)size) != 0 ||
      fstat(image_fd, &metadata) != 0 || metadata.st_size != (off_t)size ||
      require_private_policy(image_fd, S_IFREG, IMAGE_MODE, 1, 1) !=
          EXIT_SUCCESS ||
      fsync(image_fd) != 0 || fsync(parent_fd) != 0 ||
      require_private_policy(parent_fd, S_IFDIR, DIRECTORY_MODE, 0, 1) !=
          EXIT_SUCCESS) {
    status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
    goto cleanup;
  }
  if (printf("{\"device\":\"%" PRIuMAX "\",\"inode\":\"%" PRIuMAX
             "\",\"status\":\"ok\"}\n",
             (uintmax_t)metadata.st_dev, (uintmax_t)metadata.st_ino) < 0 ||
      fflush(stdout) != 0) {
    status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  } else {
    status = EXIT_SUCCESS;
  }

cleanup:
  if (image_fd >= 0) (void)close(image_fd);
  if (parent_fd >= 0) (void)close(parent_fd);
  if (root_fd >= 0) (void)close(root_fd);
  if (created && status != EXIT_SUCCESS) return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  return status;
}

static int create_private_directory(const char *root_path,
                                    const char *relative_parent,
                                    const char *name, int exclusive,
                                    const char *parent_device,
                                    const char *parent_inode) {
  struct stat metadata;
  int root_fd = -1;
  int parent_fd = -1;
  int directory_fd = -1;
  int created = 0;
  int status;
  if (!valid_direct_name(name)) return INSPECTOR_EXIT_USAGE;
  status = open_operation_parent(root_path, relative_parent, &root_fd,
                                 &parent_fd);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_fd_identity(parent_fd, parent_device, parent_inode, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  if (mkdirat(parent_fd, name, DIRECTORY_MODE) == 0) {
    created = 1;
  } else if (errno != EEXIST || exclusive) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  directory_fd = open_direct_child(parent_fd, name,
                                   O_RDONLY | O_DIRECTORY, 0);
  if (directory_fd < 0 ||
      (created && fchmod(directory_fd, DIRECTORY_MODE) != 0) ||
      require_private_policy(directory_fd, S_IFDIR, DIRECTORY_MODE, 0,
                             created) != EXIT_SUCCESS ||
      fstat(directory_fd, &metadata) != 0 ||
      (created && (fsync(directory_fd) != 0 || fsync(parent_fd) != 0)) ||
      require_private_policy(parent_fd, S_IFDIR, DIRECTORY_MODE, 0,
                             created) != EXIT_SUCCESS) {
    status = created ? INSPECTOR_EXIT_OUTCOME_UNCERTAIN
                     : INSPECTOR_EXIT_MISMATCH;
    goto cleanup;
  }
  if (printf("{\"created\":%s,\"device\":\"%" PRIuMAX
             "\",\"inode\":\"%" PRIuMAX
             "\",\"status\":\"ok\"}\n",
             created ? "true" : "false", (uintmax_t)metadata.st_dev,
             (uintmax_t)metadata.st_ino) < 0 ||
      fflush(stdout) != 0) {
    status = created ? INSPECTOR_EXIT_OUTCOME_UNCERTAIN : INSPECTOR_EXIT_IO;
    goto cleanup;
  }
  status = EXIT_SUCCESS;

cleanup:
  if (directory_fd >= 0) (void)close(directory_fd);
  if (parent_fd >= 0) (void)close(parent_fd);
  if (root_fd >= 0) (void)close(root_fd);
  if (created && status != EXIT_SUCCESS) return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  return status;
}

static int format_ext4_image(const char *root_path,
                             const char *relative_parent, const char *name,
                             const char *executable,
                             const char *parent_device,
                             const char *parent_inode,
                             const char *device, const char *inode) {
  int root_fd = -1;
  int parent_fd = -1;
  int image_fd = -1;
  int status;
  uint64_t hard_deadline;
  if (deadline_after(FORMATTER_OPERATION_TIMEOUT_MS, &hard_deadline) != 0) {
    return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  }
  if (!valid_direct_name(name) || !valid_absolute_executable(executable)) {
    return INSPECTOR_EXIT_USAGE;
  }
  status = open_operation_parent(root_path, relative_parent, &root_fd,
                                 &parent_fd);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_fd_identity(parent_fd, parent_device, parent_inode, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  image_fd = open_direct_child(parent_fd, name, O_RDWR, 0);
  if (image_fd < 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  status = require_private_policy(image_fd, S_IFREG, IMAGE_MODE, 1, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_fd_identity(image_fd, device, inode, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = run_mkfs_on_fd(executable, image_fd, hard_deadline);
  if (status != EXIT_SUCCESS) goto cleanup;
  if (require_private_policy(image_fd, S_IFREG, IMAGE_MODE, 1, 1) !=
          EXIT_SUCCESS ||
      require_private_policy(parent_fd, S_IFDIR, DIRECTORY_MODE, 0, 1) !=
          EXIT_SUCCESS ||
      fsync(image_fd) != 0) {
    status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
    goto cleanup;
  }
  status = print_ok();

cleanup:
  if (image_fd >= 0) (void)close(image_fd);
  if (parent_fd >= 0) (void)close(parent_fd);
  if (root_fd >= 0) (void)close(root_fd);
  return status;
}

static int sync_filesystem_root(const char *root_path,
                                const char *relative_path,
                                const char *filesystem_id,
                                const char *object_id,
                                const char *device, const char *inode) {
  struct stat before;
  struct stat after;
  int root_fd = -1;
  int target_fd = -1;
  int status;
  root_fd = open_root_directory(root_path);
  if (root_fd < 0) return classify_path_errno(errno);
  target_fd = open_relative_directory(root_fd, relative_path);
  if (target_fd < 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  status = require_private_policy(target_fd, S_IFDIR, DIRECTORY_MODE, 0, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_fd_identity(target_fd, device, inode, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_persistent_identity(target_fd, filesystem_id, object_id, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  if (fstat(target_fd, &before) != 0 || syncfs(target_fd) != 0 ||
      fstat(target_fd, &after) != 0 || before.st_dev != after.st_dev ||
      before.st_ino != after.st_ino ||
      require_private_policy(target_fd, S_IFDIR, DIRECTORY_MODE, 0, 1) !=
          EXIT_SUCCESS) {
    status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
    goto cleanup;
  }
  status = print_ok();

cleanup:
  if (target_fd >= 0) (void)close(target_fd);
  if (root_fd >= 0) (void)close(root_fd);
  return status;
}

static int provision_control_root(const char *root_path,
                                  const char *relative_path,
                                  const char *kind,
                                  const char *filesystem_id,
                                  const char *root_object_id,
                                  const char *expected_control_filesystem_id,
                                  const char *expected_control_object_id,
                                  const char *device, const char *inode) {
  const char *name;
  char filesystem_uuid[37];
  char object_id[73];
  char post_filesystem_uuid[37];
  char post_object_id[73];
  char visible_filesystem_uuid[37];
  char visible_object_id[73];
  struct stat metadata;
  struct stat visible_metadata;
  int root_fd = -1;
  int directory_fd = -1;
  int control_fd = -1;
  int visible_control_fd = -1;
  int created = 0;
  int dispatched = 0;
  int expects_control_identity;
  int status;
  if (strcmp(kind, "publication") == 0) {
    name = ".stopped-directory-publication.lock";
  } else if (strcmp(kind, "journal") == 0) {
    name = ".operation-journal.lock";
  } else {
    return INSPECTOR_EXIT_USAGE;
  }
  if ((strcmp(expected_control_filesystem_id, "-") == 0) !=
      (strcmp(expected_control_object_id, "-") == 0)) {
    return INSPECTOR_EXIT_USAGE;
  }
  expects_control_identity =
      strcmp(expected_control_filesystem_id, "-") != 0;
  if (expects_control_identity &&
      (strlen(expected_control_filesystem_id) != 43U ||
       strncmp(expected_control_filesystem_id, "ext4fs:", 7U) != 0 ||
       strlen(expected_control_object_id) != 72U ||
       strncmp(expected_control_object_id, "ext4fh1:", 8U) != 0)) {
    return INSPECTOR_EXIT_USAGE;
  }
  root_fd = open_root_directory(root_path);
  if (root_fd < 0) return classify_path_errno(errno);
  directory_fd = open_relative_directory(root_fd, relative_path);
  if (directory_fd < 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  status = require_private_policy(directory_fd, S_IFDIR, DIRECTORY_MODE, 0, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_fd_identity(directory_fd, device, inode, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_persistent_identity(directory_fd, filesystem_id,
                                       root_object_id, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  /* A committed identity is verification-only: absence can never create a
   * replacement inode. Null expectation is the explicit pre-commit adoption
   * mode and may create or adopt the current valid control file. */
  if (expects_control_identity) {
    control_fd = open_direct_child(directory_fd, name, O_RDWR, 0);
  } else {
    control_fd = openat(directory_fd, name,
                        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
                        CONTROL_FILE_MODE);
    if (control_fd >= 0) {
      created = 1;
    } else if (errno == EEXIST) {
      control_fd = open_direct_child(directory_fd, name, O_RDWR, 0);
    }
  }
  if (control_fd < 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  if (created) dispatched = 1;
  /*
   * Policy, emptiness, durability, runtime identity, and persistent ext4
   * identity are all derived while this single control-file FD remains pinned.
   */
  if ((created && fchmod(control_fd, CONTROL_FILE_MODE) != 0) ||
      fstat(control_fd, &metadata) != 0 || metadata.st_size != 0 ||
      require_private_policy(control_fd, S_IFREG, CONTROL_FILE_MODE, 1,
                             created) != EXIT_SUCCESS ||
      read_persistent_identity_from_fd(control_fd, filesystem_uuid,
                                       object_id) != EXIT_SUCCESS) {
    status = dispatched ? INSPECTOR_EXIT_OUTCOME_UNCERTAIN
                        : INSPECTOR_EXIT_MISMATCH;
    goto cleanup;
  }
  if (expects_control_identity &&
      (strcmp(expected_control_filesystem_id + 7U, filesystem_uuid) != 0 ||
       strcmp(expected_control_object_id, object_id) != 0)) {
    status = INSPECTOR_EXIT_MISMATCH;
    goto cleanup;
  }
  dispatched = 1;
  if (fsync(control_fd) != 0 || fsync(directory_fd) != 0 ||
      fstat(control_fd, &metadata) != 0 || metadata.st_size != 0 ||
      require_private_policy(directory_fd, S_IFDIR, DIRECTORY_MODE, 0, 1) !=
          EXIT_SUCCESS ||
      require_private_policy(control_fd, S_IFREG, CONTROL_FILE_MODE, 1, 1) !=
          EXIT_SUCCESS ||
      read_persistent_identity_from_fd(control_fd, post_filesystem_uuid,
                                       post_object_id) != EXIT_SUCCESS ||
      strcmp(post_filesystem_uuid, filesystem_uuid) != 0 ||
      strcmp(post_object_id, object_id) != 0 ||
      (visible_control_fd =
           open_direct_child(directory_fd, name, O_RDONLY, 0)) < 0 ||
      fstat(visible_control_fd, &visible_metadata) != 0 ||
      visible_metadata.st_size != 0 ||
      visible_metadata.st_dev != metadata.st_dev ||
      visible_metadata.st_ino != metadata.st_ino ||
      require_private_policy(visible_control_fd, S_IFREG, CONTROL_FILE_MODE, 1,
                             1) != EXIT_SUCCESS ||
      read_persistent_identity_from_fd(visible_control_fd,
                                       visible_filesystem_uuid,
                                       visible_object_id) != EXIT_SUCCESS ||
      strcmp(visible_filesystem_uuid, filesystem_uuid) != 0 ||
      strcmp(visible_object_id, object_id) != 0) {
    status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
    goto cleanup;
  }
  if (printf("{\"controlFileName\":\"%s\",\"created\":%s,"
             "\"device\":\"%" PRIuMAX
             "\",\"filesystemUuid\":\"%s\",\"inode\":\"%" PRIuMAX
             "\",\"kind\":\"%s\",\"objectId\":\"%s\","
             "\"status\":\"ok\"}\n",
             name, created ? "true" : "false", (uintmax_t)metadata.st_dev,
             filesystem_uuid, (uintmax_t)metadata.st_ino, kind, object_id) < 0 ||
      fflush(stdout) != 0) {
    status = dispatched ? INSPECTOR_EXIT_OUTCOME_UNCERTAIN : INSPECTOR_EXIT_IO;
    goto cleanup;
  }
  status = EXIT_SUCCESS;

cleanup:
  if (visible_control_fd >= 0) (void)close(visible_control_fd);
  if (control_fd >= 0) (void)close(control_fd);
  if (directory_fd >= 0) (void)close(directory_fd);
  if (root_fd >= 0) (void)close(root_fd);
  if (dispatched && status != EXIT_SUCCESS) {
    return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  }
  return status;
}

struct loop_receipt {
  uint64_t backing_device;
  uint64_t backing_inode;
  uint64_t size_bytes;
  unsigned int block_size;
  unsigned int device_major;
  unsigned int device_minor;
  char loop_device[64];
};

static int loop_device_path(unsigned int number, char output[64]) {
  const int length = snprintf(output, 64U, "/dev/loop%u", number);
  return length > 0 && length < 64;
}

static int parse_loop_device_path(const char *path, unsigned int *number) {
  uint64_t parsed;
  if (strncmp(path, "/dev/loop", 9U) != 0 ||
      !parse_u64_decimal(path + 9U, &parsed) || parsed >= LOOP_SCAN_LIMIT) {
    return 0;
  }
  *number = (unsigned int)parsed;
  return 1;
}

static int read_loop_receipt(int image_fd, int loop_fd,
                             const char *loop_device,
                             struct loop_receipt *receipt) {
  struct loop_info64 information;
  struct stat image_metadata;
  struct stat loop_metadata;
  uint64_t block_bytes = 0U;
  int block_size = 0;
  memset(&information, 0, sizeof(information));
  if (fstat(image_fd, &image_metadata) != 0 ||
      fstat(loop_fd, &loop_metadata) != 0 ||
      !S_ISREG(image_metadata.st_mode) || !S_ISBLK(loop_metadata.st_mode) ||
      ioctl(loop_fd, LOOP_GET_STATUS64, &information) != 0 ||
      ioctl(loop_fd, BLKSSZGET, &block_size) != 0 ||
      ioctl(loop_fd, BLKGETSIZE64, &block_bytes) != 0) {
    return -1;
  }
  /*
   * dev+ino bind the retained backing object. Offset, size limit, exact loop
   * flags, logical sector size, and full block size bind the kernel geometry;
   * the block-device rdev below binds the loop node consumed by mountinfo.
   */
  if (information.lo_device != (uint64_t)image_metadata.st_dev ||
      information.lo_inode != (uint64_t)image_metadata.st_ino ||
      information.lo_offset != 0U || information.lo_sizelimit != 0U ||
      information.lo_flags != 0U ||
      block_size != (int)LOOP_BLOCK_SIZE || image_metadata.st_size <= 0 ||
      block_bytes != (uint64_t)image_metadata.st_size) {
    errno = ESTALE;
    return 0;
  }
  memset(receipt, 0, sizeof(*receipt));
  receipt->backing_device = (uint64_t)image_metadata.st_dev;
  receipt->backing_inode = (uint64_t)image_metadata.st_ino;
  receipt->size_bytes = block_bytes;
  receipt->block_size = (unsigned int)block_size;
  receipt->device_major = major(loop_metadata.st_rdev);
  receipt->device_minor = minor(loop_metadata.st_rdev);
  if (strlen(loop_device) >= sizeof(receipt->loop_device)) {
    errno = ENAMETOOLONG;
    return 0;
  }
  memcpy(receipt->loop_device, loop_device, strlen(loop_device) + 1U);
  return 1;
}

static int print_loop_receipt(const struct loop_receipt *receipt,
                              const char *status) {
  if (printf("{\"backingDevice\":\"%" PRIu64
             "\",\"backingInode\":\"%" PRIu64
             "\",\"blockSize\":\"%u\",\"loopDevice\":\"%s\","
             "\"loopRdev\":\"%u:%u\",\"offset\":\"0\","
             "\"readOnly\":false,\"sizeBytes\":\"%" PRIu64
             "\",\"sizeLimit\":\"0\",\"status\":\"%s\"}\n",
             receipt->backing_device, receipt->backing_inode,
             receipt->block_size, receipt->loop_device,
             receipt->device_major, receipt->device_minor,
             receipt->size_bytes, status) < 0 ||
      fflush(stdout) != 0) {
    return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  }
  return EXIT_SUCCESS;
}

static int open_image_for_loop(const char *root_path,
                               const char *relative_parent,
                               const char *name, int *root_fd, int *parent_fd,
                               int *image_fd, const char *parent_device,
                               const char *parent_inode, const char *device,
                               const char *inode) {
  int status;
  if (!valid_direct_name(name)) return INSPECTOR_EXIT_USAGE;
  status = open_operation_parent(root_path, relative_parent, root_fd,
                                 parent_fd);
  if (status != EXIT_SUCCESS) return status;
  status = require_fd_identity(*parent_fd, parent_device, parent_inode, 0);
  if (status != EXIT_SUCCESS) return status;
  *image_fd = open_direct_child(*parent_fd, name, O_RDWR, 0);
  if (*image_fd < 0) return classify_path_errno(errno);
  status = require_private_policy(*image_fd, S_IFREG, IMAGE_MODE, 1, 0);
  if (status != EXIT_SUCCESS) return status;
  return require_fd_identity(*image_fd, device, inode, 0);
}

static int attach_loop_device(const char *root_path,
                              const char *relative_parent,
                              const char *name, const char *parent_device,
                              const char *parent_inode, const char *device,
                              const char *inode) {
  struct loop_config configuration;
  struct loop_receipt receipt;
  char loop_path[64];
  unsigned int attempt;
  int root_fd = -1;
  int parent_fd = -1;
  int image_fd = -1;
  int control_fd = -1;
  int loop_fd = -1;
  int configured = 0;
  int status;
  status = open_image_for_loop(root_path, relative_parent, name, &root_fd,
                               &parent_fd, &image_fd, parent_device,
                               parent_inode, device, inode);
  if (status != EXIT_SUCCESS) goto cleanup;
  control_fd = open("/dev/loop-control", O_RDWR | O_NOFOLLOW | O_CLOEXEC);
  if (control_fd < 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  for (attempt = 0U; attempt < LOOP_ATTACH_ATTEMPTS; attempt += 1U) {
    const int number = ioctl(control_fd, LOOP_CTL_GET_FREE);
    if (number < 0 || (unsigned int)number >= LOOP_SCAN_LIMIT ||
        !loop_device_path((unsigned int)number, loop_path)) {
      status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
      goto cleanup;
    }
    loop_fd = open(loop_path, O_RDWR | O_NOFOLLOW | O_CLOEXEC);
    if (loop_fd < 0) continue;
    memset(&configuration, 0, sizeof(configuration));
    configuration.fd = (uint32_t)image_fd;
    configuration.block_size = LOOP_BLOCK_SIZE;
    configuration.info.lo_offset = 0U;
    configuration.info.lo_sizelimit = 0U;
    configuration.info.lo_flags = 0U;
    if (ioctl(loop_fd, LOOP_CONFIGURE, &configuration) == 0) {
      configured = 1;
      break;
    }
    if (errno != EBUSY) {
      status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
      goto cleanup;
    }
    (void)close(loop_fd);
    loop_fd = -1;
  }
  if (!configured ||
      read_loop_receipt(image_fd, loop_fd, loop_path, &receipt) != 1 ||
      require_private_policy(image_fd, S_IFREG, IMAGE_MODE, 1, 1) !=
          EXIT_SUCCESS ||
      require_private_policy(parent_fd, S_IFDIR, DIRECTORY_MODE, 0, 1) !=
          EXIT_SUCCESS) {
    status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
    goto cleanup;
  }
  status = print_loop_receipt(&receipt, "attached");

cleanup:
  if (loop_fd >= 0) (void)close(loop_fd);
  if (control_fd >= 0) (void)close(control_fd);
  if (image_fd >= 0) (void)close(image_fd);
  if (parent_fd >= 0) (void)close(parent_fd);
  if (root_fd >= 0) (void)close(root_fd);
  if (configured && status != EXIT_SUCCESS) {
    return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  }
  return status;
}

static int find_loop_device(const char *root_path,
                            const char *relative_parent, const char *name,
                            const char *parent_device,
                            const char *parent_inode, const char *device,
                            const char *inode) {
  struct loop_receipt selected;
  char loop_path[64];
  unsigned int number;
  unsigned int matches = 0U;
  int root_fd = -1;
  int parent_fd = -1;
  int image_fd = -1;
  int status;
  status = open_image_for_loop(root_path, relative_parent, name, &root_fd,
                               &parent_fd, &image_fd, parent_device,
                               parent_inode, device, inode);
  if (status != EXIT_SUCCESS) goto cleanup;
  for (number = 0U; number < LOOP_SCAN_LIMIT; number += 1U) {
    struct loop_receipt candidate;
    int loop_fd;
    int receipt_state;
    if (!loop_device_path(number, loop_path)) {
      status = INSPECTOR_EXIT_IO;
      goto cleanup;
    }
    loop_fd = open(loop_path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (loop_fd < 0) {
      if (errno == ENOENT || errno == ENXIO || errno == ENODEV) continue;
      status = classify_path_errno(errno);
      goto cleanup;
    }
    receipt_state = read_loop_receipt(image_fd, loop_fd, loop_path, &candidate);
    (void)close(loop_fd);
    if (receipt_state < 0) {
      if (errno == ENXIO) continue;
      status = INSPECTOR_EXIT_IO;
      goto cleanup;
    }
    if (receipt_state == 0) continue;
    selected = candidate;
    matches += 1U;
    if (matches > 1U) {
      status = INSPECTOR_EXIT_MISMATCH;
      goto cleanup;
    }
  }
  if (matches == 0U) {
    if (printf("{\"status\":\"absent\"}\n") < 0 || fflush(stdout) != 0) {
      status = INSPECTOR_EXIT_IO;
    } else {
      status = EXIT_SUCCESS;
    }
  } else {
    status = print_loop_receipt(&selected, "present");
  }

cleanup:
  if (image_fd >= 0) (void)close(image_fd);
  if (parent_fd >= 0) (void)close(parent_fd);
  if (root_fd >= 0) (void)close(root_fd);
  return status;
}

static int inspect_loop_device(const char *root_path,
                               const char *relative_parent,
                               const char *name, const char *loop_path,
                               const char *parent_device,
                               const char *parent_inode, const char *device,
                               const char *inode) {
  struct loop_receipt receipt;
  int root_fd = -1;
  int parent_fd = -1;
  int image_fd = -1;
  int loop_fd = -1;
  unsigned int loop_number;
  int receipt_state;
  int status;
  if (!parse_loop_device_path(loop_path, &loop_number)) {
    return INSPECTOR_EXIT_USAGE;
  }
  (void)loop_number;
  status = open_image_for_loop(root_path, relative_parent, name, &root_fd,
                               &parent_fd, &image_fd, parent_device,
                               parent_inode, device, inode);
  if (status != EXIT_SUCCESS) goto cleanup;
  loop_fd = open(loop_path, O_RDWR | O_NOFOLLOW | O_CLOEXEC);
  if (loop_fd < 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  receipt_state = read_loop_receipt(image_fd, loop_fd, loop_path, &receipt);
  if (receipt_state != 1) {
    status = receipt_state == 0 ? INSPECTOR_EXIT_MISMATCH : INSPECTOR_EXIT_IO;
    goto cleanup;
  }
  status = print_loop_receipt(&receipt, "present");

cleanup:
  if (loop_fd >= 0) (void)close(loop_fd);
  if (image_fd >= 0) (void)close(image_fd);
  if (parent_fd >= 0) (void)close(parent_fd);
  if (root_fd >= 0) (void)close(root_fd);
  return status;
}

static int read_small_text_file(const char *path, char *buffer,
                                size_t capacity, size_t *length) {
  int fd;
  ssize_t count;
  if (capacity < 2U) return -1;
  fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
  if (fd < 0) return -1;
  count = read(fd, buffer, capacity - 1U);
  if (count < 0) {
    const int saved = errno;
    (void)close(fd);
    errno = saved;
    return -1;
  }
  if ((size_t)count == capacity - 1U) {
    char extra;
    if (read(fd, &extra, 1U) != 0) {
      (void)close(fd);
      errno = EOVERFLOW;
      return -1;
    }
  }
  if (close(fd) != 0) return -1;
  buffer[count] = '\0';
  *length = (size_t)count;
  return 0;
}

static int read_disk_sequence(unsigned int device_major,
                              unsigned int device_minor, uint64_t *sequence) {
  char path[128];
  char buffer[64];
  size_t length;
  int path_length = snprintf(path, sizeof(path),
                             "/sys/dev/block/%u:%u/diskseq", device_major,
                             device_minor);
  if (path_length <= 0 || (size_t)path_length >= sizeof(path) ||
      read_small_text_file(path, buffer, sizeof(buffer), &length) != 0 ||
      length < 2U || buffer[length - 1U] != '\n') {
    return -1;
  }
  buffer[length - 1U] = '\0';
  return parse_u64_decimal(buffer, sequence) ? 0 : -1;
}

static int loop_sysfs_backing_absent(unsigned int device_major,
                                     unsigned int device_minor) {
  char path[160];
  struct stat metadata;
  const int length = snprintf(path, sizeof(path),
                              "/sys/dev/block/%u:%u/loop/backing_file",
                              device_major, device_minor);
  if (length <= 0 || (size_t)length >= sizeof(path)) return 0;
  if (lstat(path, &metadata) == 0) return 0;
  return errno == ENOENT || errno == ENOTDIR;
}

static int detached_loop_settled(const struct loop_receipt *receipt,
                                 uint64_t prior_disk_sequence) {
  struct loop_info64 information;
  struct stat metadata;
  uint64_t current_sequence;
  int loop_fd = open(receipt->loop_device,
                     O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
  if (loop_fd < 0) return 0;
  if (fstat(loop_fd, &metadata) != 0 || !S_ISBLK(metadata.st_mode) ||
      major(metadata.st_rdev) != receipt->device_major ||
      minor(metadata.st_rdev) != receipt->device_minor) {
    (void)close(loop_fd);
    return 0;
  }
  memset(&information, 0, sizeof(information));
  if (ioctl(loop_fd, LOOP_GET_STATUS64, &information) == 0 || errno != ENXIO) {
    (void)close(loop_fd);
    return 0;
  }
  if (close(loop_fd) != 0 ||
      read_disk_sequence(receipt->device_major, receipt->device_minor,
                         &current_sequence) != 0 ||
      current_sequence <= prior_disk_sequence ||
      !loop_sysfs_backing_absent(receipt->device_major,
                                 receipt->device_minor)) {
    return 0;
  }
  /* The exact rdev is unused in-kernel, absent from loop sysfs, and at a
   * strictly newer disk sequence before teardown may complete. */
  return 1;
}

static int detach_loop_device(const char *root_path,
                              const char *relative_parent,
                              const char *name, const char *loop_path,
                              const char *parent_device,
                              const char *parent_inode, const char *device,
                              const char *inode) {
  const struct timespec interval = {.tv_sec = 0, .tv_nsec = 100000000L};
  struct loop_receipt receipt;
  uint64_t disk_sequence = 0U;
  unsigned int attempt;
  int root_fd = -1;
  int parent_fd = -1;
  int image_fd = -1;
  int loop_fd = -1;
  unsigned int loop_number;
  int receipt_state;
  int status;
  if (!parse_loop_device_path(loop_path, &loop_number)) {
    return INSPECTOR_EXIT_USAGE;
  }
  (void)loop_number;
  status = open_image_for_loop(root_path, relative_parent, name, &root_fd,
                               &parent_fd, &image_fd, parent_device,
                               parent_inode, device, inode);
  if (status != EXIT_SUCCESS) goto cleanup;
  loop_fd = open(loop_path, O_RDWR | O_NOFOLLOW | O_CLOEXEC);
  if (loop_fd < 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  receipt_state = read_loop_receipt(image_fd, loop_fd, loop_path, &receipt);
  if (receipt_state != 1 || ioctl(loop_fd, BLKGETDISKSEQ, &disk_sequence) != 0) {
    status = receipt_state == 0 ? INSPECTOR_EXIT_MISMATCH : INSPECTOR_EXIT_IO;
    goto cleanup;
  }
  if (ioctl(loop_fd, LOOP_CLR_FD, 0) != 0) {
    status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
    goto cleanup;
  }
  if (close(loop_fd) != 0) {
    loop_fd = -1;
    status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
    goto cleanup;
  }
  loop_fd = -1;
  for (attempt = 0U; attempt < LOOP_SETTLE_ATTEMPTS; attempt += 1U) {
    if (detached_loop_settled(&receipt, disk_sequence)) {
      if (require_private_policy(image_fd, S_IFREG, IMAGE_MODE, 1, 1) !=
              EXIT_SUCCESS ||
          require_private_policy(parent_fd, S_IFDIR, DIRECTORY_MODE, 0, 1) !=
              EXIT_SUCCESS) {
        status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
        goto cleanup;
      }
      status = print_ok();
      goto cleanup;
    }
    (void)nanosleep(&interval, NULL);
  }
  status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;

cleanup:
  if (loop_fd >= 0) (void)close(loop_fd);
  if (image_fd >= 0) (void)close(image_fd);
  if (parent_fd >= 0) (void)close(parent_fd);
  if (root_fd >= 0) (void)close(root_fd);
  return status;
}

static int parse_device_pair(const char *value, unsigned int *device_major,
                             unsigned int *device_minor) {
  uint64_t major_value = 0U;
  uint64_t minor_value = 0U;
  const char *separator = strchr(value, ':');
  char major_text[32];
  size_t major_length;
  if (separator == NULL || strchr(separator + 1, ':') != NULL) return 0;
  major_length = (size_t)(separator - value);
  if (major_length == 0U || major_length >= sizeof(major_text)) return 0;
  memcpy(major_text, value, major_length);
  major_text[major_length] = '\0';
  if (!parse_u64_decimal(major_text, &major_value) ||
      !parse_u64_decimal(separator + 1, &minor_value) ||
      major_value > UINT_MAX || minor_value > UINT_MAX) {
    return 0;
  }
  *device_major = (unsigned int)major_value;
  *device_minor = (unsigned int)minor_value;
  return 1;
}

static int validate_loop_geometry(int loop_fd, uint64_t backing_device,
                                  uint64_t backing_inode,
                                  uint64_t expected_size,
                                  unsigned int expected_major,
                                  unsigned int expected_minor) {
  struct loop_info64 information;
  struct stat metadata;
  uint64_t block_bytes = 0U;
  int block_size = 0;
  memset(&information, 0, sizeof(information));
  if (fstat(loop_fd, &metadata) != 0 || !S_ISBLK(metadata.st_mode) ||
      ioctl(loop_fd, LOOP_GET_STATUS64, &information) != 0 ||
      ioctl(loop_fd, BLKSSZGET, &block_size) != 0 ||
      ioctl(loop_fd, BLKGETSIZE64, &block_bytes) != 0) {
    return -1;
  }
  return information.lo_device == backing_device &&
                 information.lo_inode == backing_inode &&
                 information.lo_offset == 0U &&
                 information.lo_sizelimit == 0U &&
                 information.lo_flags == 0U &&
                 block_size == (int)LOOP_BLOCK_SIZE &&
                 block_bytes == expected_size &&
                 major(metadata.st_rdev) == expected_major &&
                 minor(metadata.st_rdev) == expected_minor
             ? 1
             : 0;
}

static int statx_mount_id(int fd, uint64_t *mount_id) {
  struct statx metadata;
  memset(&metadata, 0, sizeof(metadata));
  if (statx(fd, "", AT_EMPTY_PATH | AT_STATX_SYNC_AS_STAT, STATX_MNT_ID,
            &metadata) != 0 || (metadata.stx_mask & STATX_MNT_ID) == 0U) {
    return -1;
  }
  *mount_id = metadata.stx_mnt_id;
  return 0;
}

static int read_mountinfo(char **output, size_t *output_length) {
  char *buffer = NULL;
  size_t length = 0U;
  int fd = -1;
  int status = -1;
  fd = open("/proc/self/mountinfo", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
  if (fd < 0) goto cleanup;
  buffer = (char *)malloc(MOUNTINFO_BYTES_LIMIT + 1U);
  if (buffer == NULL) goto cleanup;
  while (length < MOUNTINFO_BYTES_LIMIT) {
    const ssize_t count =
        read(fd, buffer + length, MOUNTINFO_BYTES_LIMIT - length);
    if (count < 0) {
      if (errno == EINTR) continue;
      goto cleanup;
    }
    if (count == 0) break;
    length += (size_t)count;
  }
  if (length == MOUNTINFO_BYTES_LIMIT) {
    char extra;
    ssize_t count;
    do {
      count = read(fd, &extra, 1U);
    } while (count < 0 && errno == EINTR);
    if (count != 0) goto cleanup;
  }
  if (length == 0U || buffer[length - 1U] != '\n' ||
      memchr(buffer, '\0', length) != NULL) {
    goto cleanup;
  }
  buffer[length] = '\0';
  *output = buffer;
  *output_length = length;
  buffer = NULL;
  status = 0;

cleanup:
  free(buffer);
  if (fd >= 0 && close(fd) != 0) status = -1;
  return status;
}

static int mountinfo_token_is(const char *token, size_t length,
                              const char *expected) {
  const size_t expected_length = strlen(expected);
  return length == expected_length &&
         memcmp(token, expected, expected_length) == 0;
}

static int mountinfo_token_starts_with(const char *token, size_t length,
                                       const char *prefix) {
  const size_t prefix_length = strlen(prefix);
  return length >= prefix_length &&
         memcmp(token, prefix, prefix_length) == 0;
}

static int mount_carrier_is_private(int fd) {
  char *mountinfo = NULL;
  size_t mountinfo_length = 0U;
  uint64_t expected_mount_id;
  size_t offset = 0U;
  int found = 0;
  int isolated = 0;
  int status = -1;
  if (statx_mount_id(fd, &expected_mount_id) != 0 ||
      read_mountinfo(&mountinfo, &mountinfo_length) != 0) {
    goto cleanup;
  }
  while (offset < mountinfo_length) {
    char *line = mountinfo + offset;
    char *newline = memchr(line, '\n', mountinfo_length - offset);
    char *cursor;
    size_t field_index = 0U;
    size_t separator_index = SIZE_MAX;
    uint64_t line_mount_id = 0U;
    int non_private = 0;
    if (newline == NULL || newline == line) goto cleanup;
    *newline = '\0';
    cursor = line;
    while (*cursor != '\0') {
      char *token = cursor;
      char *end = strchr(cursor, ' ');
      size_t token_length;
      if (end == cursor) goto cleanup;
      if (end == NULL) end = cursor + strlen(cursor);
      token_length = (size_t)(end - token);
      if (field_index == 0U) {
        const char saved = *end;
        *end = '\0';
        if (!parse_u64_decimal(token, &line_mount_id)) {
          *end = saved;
          goto cleanup;
        }
        *end = saved;
      } else if (line_mount_id == expected_mount_id && field_index >= 6U &&
                 separator_index == SIZE_MAX) {
        if (mountinfo_token_is(token, token_length, "-")) {
          separator_index = field_index;
        } else if (mountinfo_token_is(token, token_length, "unbindable") ||
                   mountinfo_token_starts_with(token, token_length,
                                               "shared:") ||
                   mountinfo_token_starts_with(token, token_length,
                                               "master:") ||
                   mountinfo_token_starts_with(token, token_length,
                                               "propagate_from:")) {
          non_private = 1;
        }
      }
      field_index += 1U;
      if (*end == '\0') break;
      cursor = end + 1;
      if (*cursor == '\0') goto cleanup;
    }
    if (line_mount_id == expected_mount_id) {
      if (found || separator_index == SIZE_MAX || separator_index < 6U ||
          separator_index + 3U >= field_index) {
        goto cleanup;
      }
      found = 1;
      isolated = !non_private;
    }
    offset = (size_t)(newline - mountinfo) + 1U;
  }
  if (!found) goto cleanup;
  status = isolated ? 1 : 0;

cleanup:
  free(mountinfo);
  return status;
}

static int require_private_mount_carrier(int fd, int dispatched) {
  const int state = mount_carrier_is_private(fd);
  if (state > 0) return EXIT_SUCCESS;
  if (state == 0 && !dispatched) return INSPECTOR_EXIT_MISMATCH;
  return dispatched ? INSPECTOR_EXIT_OUTCOME_UNCERTAIN : INSPECTOR_EXIT_IO;
}

static int mount_ext4_loop(const char *root_path,
                           const char *relative_parent, const char *name,
                           const char *loop_path,
                           const char *backing_device_text,
                           const char *backing_inode_text,
                           const char *rdev_text,
                           const char *size_text,
                           const char *parent_device,
                           const char *parent_inode,
                           const char *target_device,
                           const char *target_inode) {
  static const unsigned long mount_flags =
      MS_NOSUID | MS_NODEV | MS_NOEXEC | MS_NOATIME;
  static const char mount_data[] = "errors=remount-ro";
  struct stat target_metadata;
  struct stat current_target_metadata;
  struct stat visible_metadata;
  struct statfs filesystem;
  uint64_t backing_device;
  uint64_t backing_inode;
  uint64_t expected_size;
  unsigned int expected_major;
  unsigned int expected_minor;
  unsigned int loop_number;
  char source_proc_path[64];
  char target_proc_path[64];
  int root_fd = -1;
  int parent_fd = -1;
  int target_fd = -1;
  int visible_fd = -1;
  int loop_fd = -1;
  int dispatched = 0;
  int status;
  if (!valid_direct_name(name) ||
      !parse_loop_device_path(loop_path, &loop_number) ||
      !parse_u64_decimal(backing_device_text, &backing_device) ||
      !parse_u64_decimal(backing_inode_text, &backing_inode) ||
      backing_inode == 0U || !parse_u64_decimal(size_text, &expected_size) ||
      expected_size == 0U ||
      !parse_device_pair(rdev_text, &expected_major, &expected_minor)) {
    return INSPECTOR_EXIT_USAGE;
  }
  (void)loop_number;
  status = open_operation_parent(root_path, relative_parent, &root_fd,
                                 &parent_fd);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_fd_identity(parent_fd, parent_device, parent_inode, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  target_fd = open_direct_child(parent_fd, name, O_RDONLY | O_DIRECTORY, 0);
  if (target_fd < 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  status = require_private_policy(target_fd, S_IFDIR, DIRECTORY_MODE, 0, 0);
  if (status != EXIT_SUCCESS || fstat(target_fd, &target_metadata) != 0) {
    if (status == EXIT_SUCCESS) status = INSPECTOR_EXIT_IO;
    goto cleanup;
  }
  status = require_fd_identity(target_fd, target_device, target_inode, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  loop_fd = open(loop_path, O_RDWR | O_NOFOLLOW | O_CLOEXEC);
  if (loop_fd < 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  if (validate_loop_geometry(loop_fd, backing_device, backing_inode,
                             expected_size, expected_major, expected_minor) !=
          1 ||
      !format_proc_fd_path(loop_fd, source_proc_path) ||
      !format_proc_fd_path(target_fd, target_proc_path)) {
    status = INSPECTOR_EXIT_MISMATCH;
    goto cleanup;
  }
  status = require_private_mount_carrier(parent_fd, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  dispatched = 1;
  if (mount(source_proc_path, target_proc_path, "ext4", mount_flags,
            mount_data) != 0) {
    status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
    goto cleanup;
  }
  visible_fd = open_direct_child(parent_fd, name, O_RDONLY | O_DIRECTORY, 1);
  if (visible_fd < 0 || fstat(visible_fd, &visible_metadata) != 0 ||
      fstatfs(visible_fd, &filesystem) != 0 ||
      (unsigned long)filesystem.f_type != (unsigned long)EXT4_SUPER_MAGIC ||
      major(visible_metadata.st_dev) != expected_major ||
      minor(visible_metadata.st_dev) != expected_minor ||
      fstat(target_fd, &current_target_metadata) != 0 ||
      target_metadata.st_dev != current_target_metadata.st_dev ||
      target_metadata.st_ino != current_target_metadata.st_ino ||
      !format_proc_fd_path(visible_fd, target_proc_path) ||
      require_private_mount_carrier(parent_fd, 1) != EXIT_SUCCESS ||
      mount(NULL, target_proc_path, NULL, MS_PRIVATE, NULL) != 0 ||
      fchmod(visible_fd, DIRECTORY_MODE) != 0 || syncfs(visible_fd) != 0 ||
      require_private_policy(visible_fd, S_IFDIR, DIRECTORY_MODE, 0, 1) !=
          EXIT_SUCCESS ||
      require_private_policy(parent_fd, S_IFDIR, DIRECTORY_MODE, 0, 1) !=
          EXIT_SUCCESS ||
      require_private_policy(target_fd, S_IFDIR, DIRECTORY_MODE, 0, 1) !=
          EXIT_SUCCESS) {
    status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
    goto cleanup;
  }
  status = print_ok();

cleanup:
  if (visible_fd >= 0) (void)close(visible_fd);
  if (loop_fd >= 0) (void)close(loop_fd);
  if (target_fd >= 0) (void)close(target_fd);
  if (parent_fd >= 0) (void)close(parent_fd);
  if (root_fd >= 0) (void)close(root_fd);
  if (dispatched && status != EXIT_SUCCESS) {
    return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  }
  return status;
}

static int unmount_ext4_root(const char *root_path,
                             const char *relative_parent,
                             const char *name, const char *parent_device,
                             const char *parent_inode,
                             const char *target_device,
                             const char *target_inode,
                             const char *target_filesystem_id,
                             const char *target_object_id) {
  struct statfs filesystem;
  uint64_t parent_mount_id;
  uint64_t before_mount_id;
  uint64_t after_mount_id;
  char target_proc_path[320];
  int root_fd = -1;
  int parent_fd = -1;
  int target_fd = -1;
  int host_fd = -1;
  int dispatched = 0;
  int status;
  if (!valid_direct_name(name)) return INSPECTOR_EXIT_USAGE;
  status = open_operation_parent(root_path, relative_parent, &root_fd,
                                 &parent_fd);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_fd_identity(parent_fd, parent_device, parent_inode, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_private_policy(parent_fd, S_IFDIR, DIRECTORY_MODE, 0, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  if (statx_mount_id(parent_fd, &parent_mount_id) != 0) {
    status = INSPECTOR_EXIT_IO;
    goto cleanup;
  }
  status = require_private_mount_carrier(parent_fd, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  target_fd = open_direct_child(parent_fd, name, O_RDONLY | O_DIRECTORY, 1);
  if (target_fd < 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  status = require_fd_identity(target_fd, target_device, target_inode, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_persistent_identity(target_fd, target_filesystem_id,
                                       target_object_id, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_private_policy(target_fd, S_IFDIR, DIRECTORY_MODE, 0, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  if (fstatfs(target_fd, &filesystem) != 0 ||
      statx_mount_id(target_fd, &before_mount_id) != 0) {
    status = INSPECTOR_EXIT_IO;
    goto cleanup;
  }
  if ((unsigned long)filesystem.f_type != (unsigned long)EXT4_SUPER_MAGIC ||
      before_mount_id == parent_mount_id ||
      !format_proc_fd_child_path(parent_fd, name, target_proc_path)) {
    status = INSPECTOR_EXIT_MISMATCH;
    goto cleanup;
  }
  dispatched = 1;
  if (syncfs(target_fd) != 0 ||
      require_fd_identity(target_fd, target_device, target_inode, 1) !=
          EXIT_SUCCESS ||
      require_persistent_identity(target_fd, target_filesystem_id,
                                  target_object_id, 1) != EXIT_SUCCESS ||
      require_private_policy(target_fd, S_IFDIR, DIRECTORY_MODE, 0, 1) !=
          EXIT_SUCCESS ||
      fstatfs(target_fd, &filesystem) != 0 ||
      (unsigned long)filesystem.f_type != (unsigned long)EXT4_SUPER_MAGIC ||
      statx_mount_id(target_fd, &after_mount_id) != 0 ||
      after_mount_id != before_mount_id ||
      require_fd_identity(parent_fd, parent_device, parent_inode, 1) !=
          EXIT_SUCCESS ||
      require_private_policy(parent_fd, S_IFDIR, DIRECTORY_MODE, 0, 1) !=
          EXIT_SUCCESS ||
      statx_mount_id(parent_fd, &after_mount_id) != 0 ||
      after_mount_id != parent_mount_id ||
      require_private_mount_carrier(parent_fd, 1) != EXIT_SUCCESS) {
    status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
    goto cleanup;
  }
  {
    const int closing_fd = target_fd;
    target_fd = -1;
    if (close(closing_fd) != 0 ||
        umount2(target_proc_path, UMOUNT_NOFOLLOW) != 0) {
      status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
      goto cleanup;
    }
  }
  host_fd = open_direct_child(parent_fd, name, O_RDONLY | O_DIRECTORY, 0);
  if (host_fd < 0 || statx_mount_id(host_fd, &after_mount_id) != 0 ||
      after_mount_id != parent_mount_id ||
      require_private_policy(host_fd, S_IFDIR, DIRECTORY_MODE, 0, 1) !=
          EXIT_SUCCESS ||
      require_fd_identity(parent_fd, parent_device, parent_inode, 1) !=
          EXIT_SUCCESS ||
      require_private_policy(parent_fd, S_IFDIR, DIRECTORY_MODE, 0, 1) !=
          EXIT_SUCCESS ||
      statx_mount_id(parent_fd, &after_mount_id) != 0 ||
      after_mount_id != parent_mount_id ||
      require_private_mount_carrier(parent_fd, 1) != EXIT_SUCCESS) {
    status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
    goto cleanup;
  }
  status = print_ok();

cleanup:
  if (host_fd >= 0) (void)close(host_fd);
  if (target_fd >= 0) (void)close(target_fd);
  if (parent_fd >= 0) (void)close(parent_fd);
  if (root_fd >= 0) (void)close(root_fd);
  if (dispatched && status != EXIT_SUCCESS) {
    return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  }
  return status;
}

static int remove_pinned_child(const char *root_path,
                               const char *relative_parent,
                               const char *name, int directory,
                               const char *parent_device,
                               const char *parent_inode,
                               const char *target_device,
                               const char *target_inode) {
  struct stat held_metadata;
  int root_fd = -1;
  int parent_fd = -1;
  int child_fd = -1;
  int check_fd = -1;
  int dispatched = 0;
  int status;
  if (!valid_direct_name(name)) return INSPECTOR_EXIT_USAGE;
  status = open_operation_parent(root_path, relative_parent, &root_fd,
                                 &parent_fd);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_fd_identity(parent_fd, parent_device, parent_inode, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  child_fd = open_direct_child(parent_fd, name,
                               directory ? O_RDONLY | O_DIRECTORY : O_RDONLY,
                               0);
  if (child_fd < 0) {
    status = classify_path_errno(errno);
    goto cleanup;
  }
  status = require_private_policy(child_fd, directory ? S_IFDIR : S_IFREG,
                                  directory ? DIRECTORY_MODE : IMAGE_MODE,
                                  directory ? 0 : 1, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  status = require_fd_identity(child_fd, target_device, target_inode, 0);
  if (status != EXIT_SUCCESS) goto cleanup;
  dispatched = 1;
  if (unlinkat(parent_fd, name, directory ? AT_REMOVEDIR : 0) != 0 ||
      fsync(parent_fd) != 0 || fstat(child_fd, &held_metadata) != 0 ||
      held_metadata.st_nlink != 0 ||
      unlinked_private_policy_status(
          child_fd, directory ? S_IFDIR : S_IFREG,
          directory ? DIRECTORY_MODE : IMAGE_MODE) != 1 ||
      require_private_policy(parent_fd, S_IFDIR, DIRECTORY_MODE, 0, 1) !=
          EXIT_SUCCESS) {
    status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
    goto cleanup;
  }
  check_fd = open_direct_child(parent_fd, name,
                               directory ? O_RDONLY | O_DIRECTORY : O_RDONLY,
                               0);
  if (check_fd >= 0 || errno != ENOENT) {
    status = INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
    goto cleanup;
  }
  status = print_ok();

cleanup:
  if (check_fd >= 0) (void)close(check_fd);
  if (child_fd >= 0) (void)close(child_fd);
  if (parent_fd >= 0) (void)close(parent_fd);
  if (root_fd >= 0) (void)close(root_fd);
  if (dispatched && status != EXIT_SUCCESS) {
    return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;
  }
  return status;
}

static int dispatch_operation(int argc, char **argv) {
  const char *root_path;
  const char *relative_path;
  const char *verb;
  if (argc < 8 || strcmp(argv[1], "operate") != 0 ||
      strcmp(argv[2], "--root") != 0 ||
      strcmp(argv[4], "--relative") != 0 ||
      strcmp(argv[6], "--verb") != 0) {
    return INSPECTOR_EXIT_USAGE;
  }
  root_path = argv[3];
  relative_path = argv[5];
  verb = argv[7];
  if (root_path[0] != '/' || relative_path[0] == '\0' ||
      relative_path[0] == '/') {
    return INSPECTOR_EXIT_USAGE;
  }
  if (strcmp(verb, "syncfs") == 0 && argc == 16 &&
      strcmp(argv[8], "--filesystem-id") == 0 &&
      strcmp(argv[10], "--object-id") == 0 &&
      strcmp(argv[12], "--device") == 0 &&
      strcmp(argv[14], "--inode") == 0) {
    return sync_filesystem_root(root_path, relative_path, argv[9], argv[11],
                                argv[13], argv[15]);
  }
  if (strcmp(verb, "provision-control-root") == 0 && argc == 22 &&
      strcmp(argv[8], "--kind") == 0 &&
      strcmp(argv[10], "--filesystem-id") == 0 &&
      strcmp(argv[12], "--object-id") == 0 &&
      strcmp(argv[14], "--expected-control-filesystem-id") == 0 &&
      strcmp(argv[16], "--expected-control-object-id") == 0 &&
      strcmp(argv[18], "--device") == 0 &&
      strcmp(argv[20], "--inode") == 0) {
    return provision_control_root(root_path, relative_path, argv[9], argv[11],
                                  argv[13], argv[15], argv[17], argv[19],
                                  argv[21]);
  }
  if (argc < 10 || strcmp(argv[8], "--name") != 0) {
    return INSPECTOR_EXIT_USAGE;
  }
  if (strcmp(verb, "create-image") == 0 && argc == 16 &&
      strcmp(argv[10], "--size") == 0 &&
      strcmp(argv[12], "--parent-device") == 0 &&
      strcmp(argv[14], "--parent-inode") == 0) {
    return create_image_file(root_path, relative_path, argv[9], argv[11],
                             argv[13], argv[15]);
  }
  if (strcmp(verb, "create-directory") == 0 && argc == 16 &&
      strcmp(argv[10], "--exclusive") == 0 &&
      strcmp(argv[12], "--parent-device") == 0 &&
      strcmp(argv[14], "--parent-inode") == 0 &&
      (strcmp(argv[11], "yes") == 0 || strcmp(argv[11], "no") == 0)) {
    return create_private_directory(root_path, relative_path, argv[9],
                                    strcmp(argv[11], "yes") == 0, argv[13],
                                    argv[15]);
  }
  if (strcmp(verb, "format-ext4") == 0 && argc == 20 &&
      strcmp(argv[10], "--executable") == 0 &&
      strcmp(argv[12], "--parent-device") == 0 &&
      strcmp(argv[14], "--parent-inode") == 0 &&
      strcmp(argv[16], "--device") == 0 &&
      strcmp(argv[18], "--inode") == 0) {
    return format_ext4_image(root_path, relative_path, argv[9], argv[11],
                             argv[13], argv[15], argv[17], argv[19]);
  }
  if (strcmp(verb, "attach-loop") == 0 && argc == 18 &&
      strcmp(argv[10], "--parent-device") == 0 &&
      strcmp(argv[12], "--parent-inode") == 0 &&
      strcmp(argv[14], "--device") == 0 &&
      strcmp(argv[16], "--inode") == 0) {
    return attach_loop_device(root_path, relative_path, argv[9], argv[11],
                              argv[13], argv[15], argv[17]);
  }
  if (strcmp(verb, "find-loop") == 0 && argc == 18 &&
      strcmp(argv[10], "--parent-device") == 0 &&
      strcmp(argv[12], "--parent-inode") == 0 &&
      strcmp(argv[14], "--device") == 0 &&
      strcmp(argv[16], "--inode") == 0) {
    return find_loop_device(root_path, relative_path, argv[9], argv[11],
                            argv[13], argv[15], argv[17]);
  }
  if (strcmp(verb, "inspect-loop") == 0 && argc == 20 &&
      strcmp(argv[10], "--loop") == 0 &&
      strcmp(argv[12], "--parent-device") == 0 &&
      strcmp(argv[14], "--parent-inode") == 0 &&
      strcmp(argv[16], "--device") == 0 &&
      strcmp(argv[18], "--inode") == 0) {
    return inspect_loop_device(root_path, relative_path, argv[9], argv[11],
                               argv[13], argv[15], argv[17], argv[19]);
  }
  if (strcmp(verb, "detach-loop-settle") == 0 && argc == 20 &&
      strcmp(argv[10], "--loop") == 0 &&
      strcmp(argv[12], "--parent-device") == 0 &&
      strcmp(argv[14], "--parent-inode") == 0 &&
      strcmp(argv[16], "--device") == 0 &&
      strcmp(argv[18], "--inode") == 0) {
    return detach_loop_device(root_path, relative_path, argv[9], argv[11],
                              argv[13], argv[15], argv[17], argv[19]);
  }
  if (strcmp(verb, "mount-ext4") == 0 && argc == 28 &&
      strcmp(argv[10], "--loop") == 0 &&
      strcmp(argv[12], "--backing-device") == 0 &&
      strcmp(argv[14], "--backing-inode") == 0 &&
      strcmp(argv[16], "--loop-rdev") == 0 &&
      strcmp(argv[18], "--size") == 0 &&
      strcmp(argv[20], "--parent-device") == 0 &&
      strcmp(argv[22], "--parent-inode") == 0 &&
      strcmp(argv[24], "--target-device") == 0 &&
      strcmp(argv[26], "--target-inode") == 0) {
    return mount_ext4_loop(root_path, relative_path, argv[9], argv[11],
                           argv[13], argv[15], argv[17], argv[19], argv[21],
                           argv[23], argv[25], argv[27]);
  }
  if (strcmp(verb, "unmount-ext4") == 0 && argc == 22 &&
      strcmp(argv[10], "--parent-device") == 0 &&
      strcmp(argv[12], "--parent-inode") == 0 &&
      strcmp(argv[14], "--target-device") == 0 &&
      strcmp(argv[16], "--target-inode") == 0 &&
      strcmp(argv[18], "--target-filesystem-id") == 0 &&
      strcmp(argv[20], "--target-object-id") == 0) {
    return unmount_ext4_root(root_path, relative_path, argv[9], argv[11],
                             argv[13], argv[15], argv[17], argv[19], argv[21]);
  }
  if (strcmp(verb, "remove-file") == 0 && argc == 18 &&
      strcmp(argv[10], "--parent-device") == 0 &&
      strcmp(argv[12], "--parent-inode") == 0 &&
      strcmp(argv[14], "--target-device") == 0 &&
      strcmp(argv[16], "--target-inode") == 0) {
    return remove_pinned_child(root_path, relative_path, argv[9], 0, argv[11],
                               argv[13], argv[15], argv[17]);
  }
  if (strcmp(verb, "remove-directory") == 0 && argc == 18 &&
      strcmp(argv[10], "--parent-device") == 0 &&
      strcmp(argv[12], "--parent-inode") == 0 &&
      strcmp(argv[14], "--target-device") == 0 &&
      strcmp(argv[16], "--target-inode") == 0) {
    return remove_pinned_child(root_path, relative_path, argv[9], 1, argv[11],
                               argv[13], argv[15], argv[17]);
  }
  return INSPECTOR_EXIT_USAGE;
}

int main(int argc, char **argv) {
  uid_t real_uid;
  uid_t effective_uid;
  uid_t saved_uid;
  gid_t real_gid;
  gid_t effective_gid;
  gid_t saved_gid;
  if (getresuid(&real_uid, &effective_uid, &saved_uid) != 0 ||
      getresgid(&real_gid, &effective_gid, &saved_gid) != 0 ||
      real_uid == 0 || real_uid != effective_uid || real_uid != saved_uid ||
      real_gid != effective_gid || real_gid != saved_gid) {
    return INSPECTOR_EXIT_UNSUPPORTED;
  }
  /* Authority descriptors must never be allocated into inherited stdio. */
  if (!standard_descriptors_present()) return INSPECTOR_EXIT_IO;
  if (argc == 6 && strcmp(argv[1], "inspect") == 0 &&
      strcmp(argv[2], "--root") == 0 &&
      strcmp(argv[4], "--relative") == 0) {
    return inspect_path(argv[3], argv[5]);
  }
  return dispatch_operation(argc, argv);
}

#endif
