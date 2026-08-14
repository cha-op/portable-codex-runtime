#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>

#ifndef O_NOFOLLOW
#define O_NOFOLLOW 0
#endif

static volatile sig_atomic_t stopping = 0;

static void request_stop(int signal_number) {
  (void)signal_number;
  stopping = 1;
}

static int write_all(int file_descriptor, const char *bytes, size_t length) {
  size_t offset = 0U;
  while (offset < length) {
    ssize_t written = write(file_descriptor, bytes + offset, length - offset);
    if (written < 0) {
      if (errno == EINTR) continue;
      return -1;
    }
    if (written == 0) return -1;
    offset += (size_t)written;
  }
  return 0;
}

int main(void) {
  static const char ready[] = "ready\n";
  struct sigaction action;
  sigset_t blocked_signals;
  sigset_t previous_signals;
  sigset_t wait_signals;
  int directory = -1;
  int marker = -1;

  if (sigemptyset(&blocked_signals) != 0 ||
      sigaddset(&blocked_signals, SIGINT) != 0 ||
      sigaddset(&blocked_signals, SIGTERM) != 0 ||
      sigprocmask(SIG_BLOCK, &blocked_signals, &previous_signals) != 0) {
    return 70;
  }
  wait_signals = previous_signals;
  if (sigdelset(&wait_signals, SIGINT) != 0 ||
      sigdelset(&wait_signals, SIGTERM) != 0) {
    return 70;
  }
  memset(&action, 0, sizeof(action));
  action.sa_handler = request_stop;
  if (sigemptyset(&action.sa_mask) != 0 ||
      sigaction(SIGINT, &action, NULL) != 0 ||
      sigaction(SIGTERM, &action, NULL) != 0) {
    return 70;
  }

  marker = open("/session/podman-writer-ready", O_WRONLY | O_CREAT | O_EXCL |
                                                      O_NOFOLLOW | O_CLOEXEC,
                0600);
  if (marker < 0 || write_all(marker, ready, sizeof(ready) - 1U) != 0 ||
      fsync(marker) != 0) {
    if (marker >= 0) (void)close(marker);
    return 71;
  }
  if (close(marker) != 0) {
    marker = -1;
    return 71;
  }
  marker = -1;
  directory = open("/session", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
  if (directory < 0 || fsync(directory) != 0) {
    if (directory >= 0) (void)close(directory);
    return 72;
  }
  if (close(directory) != 0) {
    directory = -1;
    return 72;
  }

  while (!stopping) (void)sigsuspend(&wait_signals);
  if (sigprocmask(SIG_SETMASK, &previous_signals, NULL) != 0) return 73;
  return 0;
}
