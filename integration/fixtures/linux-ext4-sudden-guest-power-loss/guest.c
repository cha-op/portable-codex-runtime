#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <termios.h>
#include <unistd.h>

#ifdef __linux__
#include <sys/reboot.h>
#endif

enum { PREFIX_SIZE = 4096 };

static const char MOUNT_PATH[] = "/mnt";
static const char ROLLOUT_NAME[] = "sudden-power-loss.jsonl";
static const char SESSION_LINE[] =
    "{\"timestamp\":\"2026-08-28T00:00:00.000Z\",\"type\":\"session_meta\","
    "\"payload\":{\"cli_version\":\"0.144.1\",\"cwd\":\"/workspace\","
    "\"id\":\"019f2b00-0000-7000-8000-000000000001\","
    "\"originator\":\"linux-ext4-sudden-guest-power-loss-conformance\","
    "\"session_id\":\"019f2b00-0000-7000-8000-000000000001\","
    "\"timestamp\":\"2026-08-28T00:00:00.000Z\"}}\n";
static const char EVENT_START[] =
    "{\"type\":\"event_msg\",\"payload\":{\"padding\":\"";
static const char EVENT_END[] = "\",\"sequence\":1}}\n";
static const char PARTIAL_SUFFIX[] =
    "{\"type\":\"event_msg\",\"payload\":{\"sequence\":2";
static const char ABORT_MARKER[] = "<turn_aborted>";

static const char *const ROLLOUT_COMPONENTS[] = {
    "codex-home", "sessions", "2026", "08", "28",
};

_Static_assert(sizeof(SESSION_LINE) - 1U + sizeof(EVENT_START) - 1U +
                       sizeof(EVENT_END) - 1U <=
                   PREFIX_SIZE,
               "the fixed JSONL records must fit in one prefix block");
_Static_assert(sizeof(PARTIAL_SUFFIX) > 1U,
               "the partial suffix must not be empty");

static int report_errno(const char *operation) {
  int saved_errno = errno;
  fprintf(stderr, "guest: %s: %s\n", operation, strerror(saved_errno));
  errno = saved_errno;
  return -1;
}

static int report_message(const char *message) {
  fprintf(stderr, "guest: %s\n", message);
  return -1;
}

static int close_checked(int fd) {
  if (close(fd) < 0) {
    return report_errno("close");
  }
  return 0;
}

static int fsync_checked(int fd, const char *operation) {
  for (;;) {
    if (fsync(fd) == 0) {
      return 0;
    }
    if (errno != EINTR) {
      return report_errno(operation);
    }
  }
}

static int open_mount_root(void) {
  int fd = openat(AT_FDCWD, MOUNT_PATH,
                  O_RDONLY | O_CLOEXEC | O_DIRECTORY | O_NOFOLLOW);
  if (fd < 0) {
    report_errno("open mount root");
  }
  return fd;
}

static int open_child_directory(int parent_fd, const char *name) {
  int fd = openat(parent_fd, name,
                  O_RDONLY | O_CLOEXEC | O_DIRECTORY | O_NOFOLLOW);
  if (fd < 0) {
    report_errno("open rollout directory component");
  }
  return fd;
}

static int descend_rollout_directory(int create) {
  int current_fd = open_mount_root();
  if (current_fd < 0) {
    return -1;
  }

  for (size_t index = 0U;
       index < sizeof(ROLLOUT_COMPONENTS) / sizeof(ROLLOUT_COMPONENTS[0]);
       index += 1U) {
    const char *component = ROLLOUT_COMPONENTS[index];

    if (create) {
      if (mkdirat(current_fd, component, 0700) < 0 && errno != EEXIST) {
        report_errno("create rollout directory component");
        close(current_fd);
        return -1;
      }
      if (fsync_checked(current_fd, "fsync rollout directory parent") < 0) {
        close(current_fd);
        return -1;
      }
    }

    int child_fd = open_child_directory(current_fd, component);
    if (child_fd < 0) {
      close(current_fd);
      return -1;
    }
    if (close_checked(current_fd) < 0) {
      close(child_fd);
      return -1;
    }
    current_fd = child_fd;
  }

  if (create &&
      fsync_checked(current_fd, "fsync final rollout directory") < 0) {
    close(current_fd);
    return -1;
  }
  return current_fd;
}

static int valid_nonce(const char *nonce) {
  if (strlen(nonce) != 32U) {
    return 0;
  }
  for (size_t index = 0U; index < 32U; index += 1U) {
    char byte = nonce[index];
    if (!((byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f') ||
          (byte >= 'A' && byte <= 'F'))) {
      return 0;
    }
  }
  return 1;
}

static void build_prefix(uint8_t prefix[PREFIX_SIZE]) {
  size_t offset = 0U;
  const size_t session_length = sizeof(SESSION_LINE) - 1U;
  const size_t event_start_length = sizeof(EVENT_START) - 1U;
  const size_t event_end_length = sizeof(EVENT_END) - 1U;

  memcpy(prefix + offset, SESSION_LINE, session_length);
  offset += session_length;
  memcpy(prefix + offset, EVENT_START, event_start_length);
  offset += event_start_length;
  memset(prefix + offset, 'x',
         PREFIX_SIZE - offset - event_end_length);
  offset = PREFIX_SIZE - event_end_length;
  memcpy(prefix + offset, EVENT_END, event_end_length);
}

static int write_all(int fd, const uint8_t *bytes, size_t length,
                     const char *operation) {
  size_t offset = 0U;
  while (offset < length) {
    ssize_t written = write(fd, bytes + offset, length - offset);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return report_errno(operation);
    }
    if (written == 0) {
      return report_message("write made no progress");
    }
    offset += (size_t)written;
  }
  return 0;
}

static int pread_all(int fd, uint8_t *bytes, size_t length, off_t offset) {
  size_t consumed = 0U;
  while (consumed < length) {
    ssize_t read_length =
        pread(fd, bytes + consumed, length - consumed,
              offset + (off_t)consumed);
    if (read_length < 0) {
      if (errno == EINTR) {
        continue;
      }
      return report_errno("read rollout");
    }
    if (read_length == 0) {
      return report_message("rollout ended before its declared size");
    }
    consumed += (size_t)read_length;
  }
  return 0;
}

static int contains_bytes(const uint8_t *haystack, size_t haystack_length,
                          const char *needle, size_t needle_length) {
  if (needle_length == 0U || haystack_length < needle_length) {
    return 0;
  }
  for (size_t offset = 0U; offset <= haystack_length - needle_length;
       offset += 1U) {
    if (memcmp(haystack + offset, needle, needle_length) == 0) {
      return 1;
    }
  }
  return 0;
}

static int run_setup(void) {
  int rollout_directory_fd = descend_rollout_directory(1);
  if (rollout_directory_fd < 0) {
    return EXIT_FAILURE;
  }
  if (close_checked(rollout_directory_fd) < 0) {
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}

static int emit_ready_marker(const char *nonce) {
  static const char marker_prefix[] = "\nPCR_SUDDEN_GUEST_POWER_READY_V1 ";
  uint8_t marker[sizeof(marker_prefix) - 1U + 32U + 1U];
  size_t offset = 0U;

  memcpy(marker + offset, marker_prefix, sizeof(marker_prefix) - 1U);
  offset += sizeof(marker_prefix) - 1U;
  memcpy(marker + offset, nonce, 32U);
  offset += 32U;
  marker[offset] = '\n';
  return write_all(STDOUT_FILENO, marker, sizeof(marker),
                   "write ready marker");
}

static int run_armed(const char *nonce) {
  uint8_t prefix[PREFIX_SIZE];
  build_prefix(prefix);

  int rollout_directory_fd = descend_rollout_directory(0);
  if (rollout_directory_fd < 0) {
    return EXIT_FAILURE;
  }

  int rollout_fd = openat(rollout_directory_fd, ROLLOUT_NAME,
                          O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL | O_NOFOLLOW,
                          0600);
  if (rollout_fd < 0) {
    report_errno("create rollout");
    close(rollout_directory_fd);
    return EXIT_FAILURE;
  }

  if (write_all(rollout_fd, prefix, sizeof(prefix),
                "write rollout prefix") < 0 ||
      fsync_checked(rollout_fd, "fsync rollout prefix") < 0 ||
      fsync_checked(rollout_directory_fd, "fsync rollout directory") < 0 ||
      write_all(rollout_fd, (const uint8_t *)PARTIAL_SUFFIX,
                sizeof(PARTIAL_SUFFIX) - 1U,
                "write rollout partial suffix") < 0 ||
      emit_ready_marker(nonce) < 0) {
    close(rollout_fd);
    close(rollout_directory_fd);
    return EXIT_FAILURE;
  }

  for (;;) {
    pause();
  }
}

static int run_recover(void) {
  uint8_t expected_prefix[PREFIX_SIZE];
  uint8_t observed_prefix[PREFIX_SIZE];
  uint8_t tail[sizeof(PARTIAL_SUFFIX) - 1U];
  build_prefix(expected_prefix);

  int rollout_directory_fd = descend_rollout_directory(0);
  if (rollout_directory_fd < 0) {
    return EXIT_FAILURE;
  }

  int rollout_fd = openat(rollout_directory_fd, ROLLOUT_NAME,
                          O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
  if (rollout_fd < 0) {
    report_errno("open rollout");
    close(rollout_directory_fd);
    return EXIT_FAILURE;
  }

  struct stat metadata;
  if (fstat(rollout_fd, &metadata) < 0) {
    report_errno("stat rollout");
    close(rollout_fd);
    close(rollout_directory_fd);
    return EXIT_FAILURE;
  }
  if (!S_ISREG(metadata.st_mode)) {
    report_message("rollout is not a regular file");
    close(rollout_fd);
    close(rollout_directory_fd);
    return EXIT_FAILURE;
  }

  const off_t minimum_size = (off_t)PREFIX_SIZE;
  const off_t maximum_size =
      minimum_size + (off_t)(sizeof(PARTIAL_SUFFIX) - 1U);
  if (metadata.st_size < minimum_size || metadata.st_size > maximum_size) {
    report_message("rollout size is outside the crash-prefix boundary");
    close(rollout_fd);
    close(rollout_directory_fd);
    return EXIT_FAILURE;
  }

  if (pread_all(rollout_fd, observed_prefix, sizeof(observed_prefix), 0) < 0) {
    close(rollout_fd);
    close(rollout_directory_fd);
    return EXIT_FAILURE;
  }
  if (memcmp(observed_prefix, expected_prefix, sizeof(expected_prefix)) != 0) {
    report_message("rollout prefix differs from the fsynced JSONL block");
    close(rollout_fd);
    close(rollout_directory_fd);
    return EXIT_FAILURE;
  }

  size_t tail_length = (size_t)(metadata.st_size - minimum_size);
  if (tail_length > 0U &&
      pread_all(rollout_fd, tail, tail_length, minimum_size) < 0) {
    close(rollout_fd);
    close(rollout_directory_fd);
    return EXIT_FAILURE;
  }
  for (size_t index = 0U; index < tail_length; index += 1U) {
    if (tail[index] == (uint8_t)'\n') {
      report_message("rollout tail contains a completed JSONL record");
      close(rollout_fd);
      close(rollout_directory_fd);
      return EXIT_FAILURE;
    }
  }
  if (contains_bytes(tail, tail_length, ABORT_MARKER,
                     sizeof(ABORT_MARKER) - 1U)) {
    report_message("rollout tail contains an abort marker");
    close(rollout_fd);
    close(rollout_directory_fd);
    return EXIT_FAILURE;
  }

  if (close_checked(rollout_fd) < 0 ||
      close_checked(rollout_directory_fd) < 0) {
    return EXIT_FAILURE;
  }
  if (printf("%zu\n", tail_length) < 0 || fflush(stdout) == EOF) {
    report_errno("write recovered tail length");
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}

static int run_poweroff(void) {
#ifdef __linux__
  if (reboot(RB_POWER_OFF) < 0) {
    return report_errno("power off guest");
  }
  return report_message("power-off syscall returned unexpectedly");
#else
  return report_message("power off is supported only by the Linux guest");
#endif
}

static int run_drain(void) {
  for (;;) {
    if (tcdrain(STDOUT_FILENO) == 0) {
      return EXIT_SUCCESS;
    }
    if (errno != EINTR) {
      report_errno("drain guest serial output");
      return EXIT_FAILURE;
    }
  }
}

int main(int argc, char **argv) {
  umask(0077);
  if (argc == 2) {
    if (strcmp(argv[1], "poweroff") == 0) {
      return run_poweroff();
    }
    if (strcmp(argv[1], "drain") == 0) {
      return run_drain();
    }
  }
  if (argc != 3) {
    report_message(
        "usage: guest <setup|armed|recover> <32-hex-nonce> | "
        "guest <drain|poweroff>");
    return EXIT_FAILURE;
  }
  if (!valid_nonce(argv[2])) {
    report_message("nonce must contain exactly 32 hexadecimal characters");
    return EXIT_FAILURE;
  }
  if (strcmp(argv[1], "setup") == 0) {
    return run_setup();
  }
  if (strcmp(argv[1], "armed") == 0) {
    return run_armed(argv[2]);
  }
  if (strcmp(argv[1], "recover") == 0) {
    return run_recover();
  }
  report_message("mode must be setup, armed, or recover");
  return EXIT_FAILURE;
}
