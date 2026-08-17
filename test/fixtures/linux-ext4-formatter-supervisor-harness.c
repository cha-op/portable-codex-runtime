#define PORTABLE_CODEX_FORMATTER_SUPERVISOR_TEST 1
#include "../../native/linux-ext4-inspector.c"

#include <fcntl.h>
#include <poll.h>

static int formatter_test_trace_fd = -1;

static void formatter_test_ignore_term(int signal_number) {
  const char trace = 'T';
  (void)signal_number;
  if (formatter_test_trace_fd >= 0) {
    ssize_t written = write(formatter_test_trace_fd, &trace, 1U);
    (void)written;
  }
}

static void formatter_test_interrupt(int signal_number) {
  (void)signal_number;
}

static int formatter_test_read_exact(int fd, void *buffer, size_t length,
                                     uint64_t timeout_ms) {
  size_t offset = 0U;
  uint64_t deadline;
  int flags = fcntl(fd, F_GETFL);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0 ||
      deadline_after(timeout_ms, &deadline) != 0) {
    return -1;
  }
  while (offset < length) {
    ssize_t bytes = read(fd, (char *)buffer + offset, length - offset);
    if (bytes > 0) {
      offset += (size_t)bytes;
      continue;
    }
    if (bytes == 0) return -1;
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      struct pollfd descriptor = {fd, POLLIN | POLLHUP, 0};
      uint64_t now;
      int polled;
      if (monotonic_milliseconds(&now) != 0 || now >= deadline) return -1;
      do {
        polled = poll(&descriptor, 1, 20);
      } while (polled < 0 && errno == EINTR);
      if (polled < 0) return -1;
      continue;
    }
    return -1;
  }
  return 0;
}

static int formatter_test_wait_reap(pid_t child, uint64_t timeout_ms,
                                    int *wait_status) {
  uint64_t deadline;
  if (deadline_after(timeout_ms, &deadline) != 0) return -1;
  for (;;) {
    struct timespec delay = {0, 5000000L};
    uint64_t now;
    pid_t waited = waitpid(child, wait_status, WNOHANG);
    if (waited == child) return 0;
    if (waited < 0) {
      if (errno == EINTR) continue;
      return -1;
    }
    if (monotonic_milliseconds(&now) != 0 || now >= deadline) return -1;
    while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
    }
  }
}

static int formatter_test_cleanup_child(pid_t *child, int process_group) {
  int wait_status;
  pid_t waited;
  if (*child <= 0) return 0;
  do {
    waited = waitpid(*child, &wait_status, WNOHANG);
  } while (waited < 0 && errno == EINTR);
  if (waited == *child || (waited < 0 && errno == ECHILD)) {
    *child = -1;
    return 0;
  }
  if (waited < 0) return -1;
  if (process_group) (void)kill(-*child, SIGKILL);
  (void)kill(*child, SIGKILL);
  if (formatter_test_wait_reap(*child, UINT64_C(1000), &wait_status) != 0) {
    return -1;
  }
  *child = -1;
  return 0;
}

static int formatter_test_exit_case(int exec_failure, int exit_status) {
  static char *const empty_environment[] = {NULL};
  char *const missing_arguments[] = {(char *)"missing-formatter", NULL};
  int group_established = 0;
  int result = -1;
  int wait_status = 0;
  int supervision;
  uint64_t hard_deadline;
  pid_t expected_parent = getpid();
  pid_t child = fork();
  (void)expected_parent;
  if (child < 0) return -1;
  if (child == 0) {
#if defined(__linux__)
    if (configure_formatter_parent_death(expected_parent) != 0) _exit(29);
#endif
    if (setpgid(0, 0) != 0) _exit(30);
    if (exec_failure) {
      execve("/portable-codex-missing-formatter", missing_arguments,
             empty_environment);
      _exit(errno == ENOENT ? 127 : 126);
    }
    _exit(exit_status);
  }
  if (establish_formatter_process_group(child) != 0) goto cleanup;
  group_established = 1;
  if (deadline_after(UINT64_C(2040), &hard_deadline) != 0) goto cleanup;
  supervision = supervise_formatter(child, hard_deadline, UINT64_C(1000),
                                    UINT64_C(40), UINT64_C(1000),
                                    &wait_status);
  if (supervision != FORMATTER_SUPERVISION_REAPED ||
      !WIFEXITED(wait_status) ||
      WEXITSTATUS(wait_status) != (exec_failure ? 127 : exit_status)) {
    goto cleanup;
  }
  errno = 0;
  if (waitpid(child, &wait_status, WNOHANG) == -1 && errno == ECHILD) {
    child = -1;
    result = 0;
  }

cleanup:
  if (formatter_test_cleanup_child(&child, group_established) != 0) result = -1;
  return result;
}

#if defined(__linux__)
struct formatter_test_tree {
  int fence_fd;
  int trace_fd;
  pid_t leader;
  pid_t writer;
};

struct formatter_test_ready {
  pid_t writer;
  char marker;
};

static int formatter_test_spawn_tree(int image_fd,
                                     struct formatter_test_tree *tree) {
  int fence_pipe[2] = {-1, -1};
  int trace_pipe[2] = {-1, -1};
  struct pollfd ready_poll;
  struct formatter_test_ready ready;
  pid_t expected_parent = getpid();
  pid_t leader;
  memset(tree, 0, sizeof(*tree));
  tree->fence_fd = -1;
  tree->trace_fd = -1;
  if (pipe(trace_pipe) != 0 || pipe(fence_pipe) != 0) {
    if (trace_pipe[0] >= 0) (void)close(trace_pipe[0]);
    if (trace_pipe[1] >= 0) (void)close(trace_pipe[1]);
    return -1;
  }
  leader = fork();
  if (leader < 0) {
    (void)close(trace_pipe[0]);
    (void)close(trace_pipe[1]);
    (void)close(fence_pipe[0]);
    (void)close(fence_pipe[1]);
    return -1;
  }
  if (leader == 0) {
    struct sigaction ignore_term;
    pid_t writer;
    if (configure_formatter_parent_death(expected_parent) != 0) _exit(39);
    if (setpgid(0, 0) != 0) _exit(40);
    memset(&ignore_term, 0, sizeof(ignore_term));
    ignore_term.sa_handler = SIG_IGN;
    if (sigemptyset(&ignore_term.sa_mask) != 0 ||
        sigaction(SIGTERM, &ignore_term, NULL) != 0) {
      _exit(41);
    }
    (void)close(trace_pipe[0]);
    (void)close(fence_pipe[1]);
    writer = fork();
    if (writer < 0) _exit(42);
    if (writer == 0) {
      char fence_probe;
      int fence_flags;
      struct sigaction ignore_term_override;
      uint64_t counter = UINT64_C(1);
      memset(&ignore_term_override, 0, sizeof(ignore_term_override));
      ignore_term_override.sa_handler = formatter_test_ignore_term;
      formatter_test_trace_fd = trace_pipe[1];
      fence_flags = fcntl(fence_pipe[0], F_GETFL);
      if (sigemptyset(&ignore_term_override.sa_mask) != 0 ||
          sigaction(SIGTERM, &ignore_term_override, NULL) != 0 ||
          fence_flags < 0 ||
          fcntl(fence_pipe[0], F_SETFL, fence_flags | O_NONBLOCK) != 0 ||
          pwrite(image_fd, &counter, sizeof(counter), 0) !=
              (ssize_t)sizeof(counter)) {
        _exit(43);
      }
      memset(&ready, 0, sizeof(ready));
      ready.writer = getpid();
      ready.marker = 'R';
      if (write(trace_pipe[1], &ready, sizeof(ready)) !=
          (ssize_t)sizeof(ready)) {
        _exit(44);
      }
      for (;;) {
        ssize_t fence_state = read(fence_pipe[0], &fence_probe, 1U);
        if (fence_state == 0) _exit(0);
        if (fence_state < 0 && errno != EINTR && errno != EAGAIN &&
            errno != EWOULDBLOCK) {
          _exit(46);
        }
        counter += UINT64_C(1);
        if (pwrite(image_fd, &counter, sizeof(counter), 0) !=
            (ssize_t)sizeof(counter)) {
          _exit(45);
        }
      }
    }
    (void)close(fence_pipe[0]);
    (void)close(trace_pipe[1]);
    (void)close(image_fd);
    for (;;) pause();
  }

  (void)close(trace_pipe[1]);
  (void)close(fence_pipe[0]);
  tree->leader = leader;
  tree->fence_fd = fence_pipe[1];
  tree->trace_fd = trace_pipe[0];
  ready_poll.fd = trace_pipe[0];
  ready_poll.events = POLLIN | POLLHUP;
  ready_poll.revents = 0;
  if (poll(&ready_poll, 1, 1000) <= 0) return -1;
  if (establish_formatter_process_group(leader) != 0 ||
      formatter_test_read_exact(trace_pipe[0], &ready, sizeof(ready),
                                UINT64_C(1000)) != 0 ||
      ready.marker != 'R' || ready.writer <= 0 ||
      getpgid(ready.writer) != leader) {
    return -1;
  }
  tree->writer = ready.writer;
  return 0;
}

static int formatter_test_trace_eof(int fd, int expect_term) {
  char buffer[16];
  int flags = fcntl(fd, F_GETFL);
  int saw_term = 0;
  uint64_t deadline;
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0 ||
      deadline_after(UINT64_C(1000), &deadline) != 0) {
    return -1;
  }
  for (;;) {
    ssize_t bytes = read(fd, buffer, sizeof(buffer));
    if (bytes > 0) {
      ssize_t index;
      for (index = 0; index < bytes; index += 1) {
        if (buffer[index] == 'T') saw_term = 1;
      }
      continue;
    }
    if (bytes == 0) return saw_term == expect_term ? 0 : -1;
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      struct pollfd descriptor = {fd, POLLIN | POLLHUP, 0};
      uint64_t now;
      int polled;
      if (monotonic_milliseconds(&now) != 0 || now >= deadline) return -1;
      do {
        polled = poll(&descriptor, 1, 20);
      } while (polled < 0 && errno == EINTR);
      if (polled < 0) return -1;
      continue;
    }
    return -1;
  }
}

static int formatter_test_cleanup_tree(struct formatter_test_tree *tree) {
  char buffer[32];
  int leader_reaped = tree->leader <= 0;
  int pipe_closed = tree->trace_fd < 0;
  int wait_status;
  uint64_t deadline;

  if (tree->fence_fd >= 0) (void)close(tree->fence_fd);
  tree->fence_fd = -1;

  if (tree->trace_fd >= 0) {
    int flags = fcntl(tree->trace_fd, F_GETFL);
    if (flags >= 0) {
      (void)fcntl(tree->trace_fd, F_SETFL, flags | O_NONBLOCK);
    }
    errno = 0;
    if (read(tree->trace_fd, buffer, sizeof(buffer)) == 0) pipe_closed = 1;
  }
  if (!leader_reaped) {
    pid_t waited = waitpid(tree->leader, &wait_status, WNOHANG);
    if (waited == tree->leader || (waited < 0 && errno == ECHILD)) {
      leader_reaped = 1;
    } else if (waited == 0) {
      (void)kill(-tree->leader, SIGKILL);
    }
  }
  if (deadline_after(UINT64_C(1000), &deadline) == 0) {
    while (!leader_reaped) {
      uint64_t now;
      pid_t waited = waitpid(tree->leader, &wait_status, WNOHANG);
      if (waited == tree->leader || (waited < 0 && errno == ECHILD)) {
        leader_reaped = 1;
        break;
      }
      if (waited < 0 || monotonic_milliseconds(&now) != 0 ||
          now >= deadline) {
        break;
      }
      {
        struct timespec delay = {0, 5000000L};
        while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
        }
      }
    }
    while (!pipe_closed && tree->trace_fd >= 0) {
      struct pollfd descriptor = {tree->trace_fd, POLLIN | POLLHUP, 0};
      uint64_t now;
      ssize_t bytes = read(tree->trace_fd, buffer, sizeof(buffer));
      if (bytes == 0) {
        pipe_closed = 1;
        break;
      }
      if (bytes < 0 && errno != EAGAIN && errno != EWOULDBLOCK &&
          errno != EINTR) {
        break;
      }
      if (monotonic_milliseconds(&now) != 0 || now >= deadline ||
          poll(&descriptor, 1, 20) < 0) {
        break;
      }
    }
  }
  if (tree->trace_fd >= 0) (void)close(tree->trace_fd);
  tree->trace_fd = -1;
  tree->leader = -1;
  tree->writer = -1;
  return leader_reaped && pipe_closed ? 0 : -1;
}

static int formatter_test_descendant_group(int image_fd) {
  struct formatter_test_tree positive;
  struct formatter_test_tree negative;
  struct timespec settle = {0, 50000000L};
  uint64_t before;
  uint64_t after;
  uint64_t hard_deadline;
  uint64_t group_deadline;
  int wait_status = 0;
  int result = -1;
  int supervision;
  pid_t negative_group = -1;

  memset(&positive, 0, sizeof(positive));
  memset(&negative, 0, sizeof(negative));
  positive.fence_fd = -1;
  positive.trace_fd = -1;
  negative.fence_fd = -1;
  negative.trace_fd = -1;

  if (ftruncate(image_fd, 0) != 0 ||
      formatter_test_spawn_tree(image_fd, &positive) != 0 ||
      deadline_after(UINT64_C(1060), &hard_deadline) != 0) {
    goto cleanup;
  }
  supervision = supervise_formatter(
      positive.leader, hard_deadline, UINT64_C(20), UINT64_C(40),
      UINT64_C(1000), &wait_status);
  if (supervision != FORMATTER_SUPERVISION_TIMED_OUT ||
      !WIFSIGNALED(wait_status) || WTERMSIG(wait_status) != SIGKILL) {
    goto cleanup;
  }
  positive.leader = -1;
  if (formatter_test_trace_eof(positive.trace_fd, 1) != 0 ||
      pread(image_fd, &before, sizeof(before), 0) != (ssize_t)sizeof(before)) {
    goto cleanup;
  }
  while (nanosleep(&settle, &settle) != 0 && errno == EINTR) {
  }
  if (pread(image_fd, &after, sizeof(after), 0) != (ssize_t)sizeof(after) ||
      before != after || close(positive.trace_fd) != 0) {
    goto cleanup;
  }
  positive.trace_fd = -1;
  if (close(positive.fence_fd) != 0) goto cleanup;
  positive.fence_fd = -1;

  if (ftruncate(image_fd, 0) != 0 ||
      formatter_test_spawn_tree(image_fd, &negative) != 0) {
    goto cleanup;
  }
  negative_group = negative.leader;
  if (kill(negative.leader, SIGKILL) != 0 ||
      formatter_test_wait_reap(negative.leader, UINT64_C(1000),
                               &wait_status) != 0 ||
      !WIFSIGNALED(wait_status) ||
      WTERMSIG(wait_status) != SIGKILL ||
      pread(image_fd, &before, sizeof(before), 0) != (ssize_t)sizeof(before)) {
    goto cleanup;
  }
  negative.leader = -1;
  settle.tv_sec = 0;
  settle.tv_nsec = 50000000L;
  while (nanosleep(&settle, &settle) != 0 && errno == EINTR) {
  }
  if (pread(image_fd, &after, sizeof(after), 0) != (ssize_t)sizeof(after) ||
      before == after || fcntl(negative.trace_fd, F_SETFL, O_NONBLOCK) != 0) {
    goto cleanup;
  }
  errno = 0;
  if (read(negative.trace_fd, &after, 1U) != -1 ||
      (errno != EAGAIN && errno != EWOULDBLOCK) ||
      getpgid(negative.writer) != negative_group ||
      kill(-negative_group, SIGKILL) != 0 ||
      formatter_test_trace_eof(negative.trace_fd, 0) != 0 ||
      deadline_after(UINT64_C(1000), &group_deadline) != 0 ||
      formatter_group_quiescent_until(negative_group, group_deadline) <= 0 ||
      close(negative.trace_fd) != 0) {
    goto cleanup;
  }
  negative.trace_fd = -1;
  result = 0;

cleanup:
  if (formatter_test_cleanup_tree(&positive) != 0) result = -1;
  if (formatter_test_cleanup_tree(&negative) != 0) result = -1;
  return result;
}
#endif

#if defined(__linux__) && defined(PR_SET_CHILD_SUBREAPER)
static int formatter_test_parent_death(void) {
  int pid_pipe[2] = {-1, -1};
  int prior_subreaper = 0;
  int result = -1;
  int subreaper_changed = 0;
  int wait_status = 0;
  char eof_probe;
  pid_t formatter = -1;
  pid_t helper = -1;
  pid_t expected_parent = getpid();

  if (prctl(PR_GET_CHILD_SUBREAPER, &prior_subreaper) != 0 ||
      prctl(PR_SET_CHILD_SUBREAPER, 1) != 0) {
    goto cleanup;
  }
  subreaper_changed = 1;
  if (pipe(pid_pipe) != 0) goto cleanup;
  helper = fork();
  if (helper < 0) goto cleanup;
  if (helper == 0) {
    const pid_t formatter_parent = getpid();
    pid_t child;
    (void)close(pid_pipe[0]);
    if (configure_formatter_parent_death(expected_parent) != 0) _exit(19);
    child = fork();
    if (child < 0) _exit(20);
    if (child == 0) {
      const pid_t self = getpid();
      if (configure_formatter_parent_death(formatter_parent) != 0 ||
          setpgid(0, 0) != 0 ||
          write(pid_pipe[1], &self, sizeof(self)) != (ssize_t)sizeof(self)) {
        _exit(21);
      }
      for (;;) pause();
    }
    if (establish_formatter_process_group(child) != 0) _exit(22);
    (void)close(pid_pipe[1]);
    for (;;) pause();
  }

  (void)close(pid_pipe[1]);
  pid_pipe[1] = -1;
  if (formatter_test_read_exact(pid_pipe[0], &formatter, sizeof(formatter),
                                UINT64_C(1000)) != 0 ||
      formatter <= 0 || kill(helper, SIGKILL) != 0) {
    goto cleanup;
  }
  if (formatter_test_wait_reap(helper, UINT64_C(1000), &wait_status) != 0 ||
      !WIFSIGNALED(wait_status) ||
      WTERMSIG(wait_status) != SIGKILL ||
      formatter_test_wait_reap(formatter, UINT64_C(1000), &wait_status) != 0 ||
      !WIFSIGNALED(wait_status) || WTERMSIG(wait_status) != SIGKILL) {
    goto cleanup;
  }
  helper = -1;
  formatter = -1;
  if (read(pid_pipe[0], &eof_probe, 1U) != 0) goto cleanup;
  result = 0;

cleanup:
  if (formatter_test_cleanup_child(&helper, 0) != 0) result = -1;
  if (formatter_test_cleanup_child(&formatter, 1) != 0) result = -1;
  if (pid_pipe[0] >= 0 && close(pid_pipe[0]) != 0) result = -1;
  if (pid_pipe[1] >= 0 && close(pid_pipe[1]) != 0) result = -1;
  if (subreaper_changed &&
      prctl(PR_SET_CHILD_SUBREAPER, prior_subreaper) != 0) {
    result = -1;
  }
  return result;
}
#endif

int main(void) {
  char image_path[] = "/tmp/portable-codex-formatter-test.XXXXXX";
  const char ready = 'R';
  char observed = '\0';
  char before = '\0';
  char after = '\0';
  struct sigaction action;
  struct sigaction interrupt_action;
  struct timespec settle = {0, 50000000L};
  int image_fd = -1;
  int image_unlinked = 0;
  int trace_pipe[2] = {-1, -1};
  int child_group_established = 0;
  int descendant_group_verified = 0;
  int result = 1;
  int wait_status = 0;
  int supervision;
  int parent_death_verified = 0;
  uint64_t hard_deadline;
  pid_t child = -1;
  pid_t expected_parent = getpid();

  (void)expected_parent;

  image_fd = mkstemp(image_path);
  memset(&interrupt_action, 0, sizeof(interrupt_action));
  interrupt_action.sa_handler = formatter_test_interrupt;
  if (image_fd < 0 || unlink(image_path) != 0 || pipe(trace_pipe) != 0 ||
      sigemptyset(&interrupt_action.sa_mask) != 0 ||
      sigaction(SIGUSR1, &interrupt_action, NULL) != 0) {
    goto cleanup;
  }
  image_unlinked = 1;
  child = fork();
  if (child < 0) {
    result = 2;
    goto cleanup;
  }
  if (child == 0) {
    char value = 'a';
    struct timespec interrupt_delay = {0, 10000000L};
#if defined(__linux__)
    if (configure_formatter_parent_death(expected_parent) != 0) _exit(1);
#endif
    if (setpgid(0, 0) != 0) _exit(2);
    (void)close(trace_pipe[0]);
    formatter_test_trace_fd = trace_pipe[1];
    memset(&action, 0, sizeof(action));
    action.sa_handler = formatter_test_ignore_term;
    if (sigemptyset(&action.sa_mask) != 0 ||
        sigaction(SIGTERM, &action, NULL) != 0 ||
        write(trace_pipe[1], &ready, 1U) != 1) {
      _exit(3);
    }
    while (nanosleep(&interrupt_delay, &interrupt_delay) != 0 &&
           errno == EINTR) {
    }
    if (kill(getppid(), SIGUSR1) != 0) _exit(4);
    for (;;) {
      if (pwrite(image_fd, &value, 1U, 0) != 1) _exit(4);
      value = value == 'z' ? 'a' : (char)(value + 1);
    }
  }

  if (establish_formatter_process_group(child) != 0) {
    result = 3;
    goto cleanup;
  }
  child_group_established = 1;
  (void)close(trace_pipe[1]);
  trace_pipe[1] = -1;
  if (formatter_test_read_exact(trace_pipe[0], &observed, 1U,
                                UINT64_C(1000)) != 0 ||
      observed != ready) {
    result = 5;
    goto cleanup;
  }
  if (deadline_after(UINT64_C(1060), &hard_deadline) != 0) {
    result = 6;
    goto cleanup;
  }
  supervision = supervise_formatter(child, hard_deadline, UINT64_C(20),
                                    UINT64_C(40), UINT64_C(1000),
                                    &wait_status);
  if (supervision != FORMATTER_SUPERVISION_TIMED_OUT ||
      !WIFSIGNALED(wait_status) || WTERMSIG(wait_status) != SIGKILL) {
    (void)fprintf(stderr, "supervision=%d wait_status=%d errno=%d\n",
                  supervision, wait_status, errno);
    result = 6;
    goto cleanup;
  }
  errno = 0;
  if (waitpid(child, &wait_status, WNOHANG) != -1 || errno != ECHILD) {
    result = 7;
    goto cleanup;
  }
  child = -1;
  if (formatter_test_read_exact(trace_pipe[0], &observed, 1U,
                                UINT64_C(1000)) != 0 ||
      observed != 'T' ||
      read(trace_pipe[0], &observed, 1U) != 0) {
    result = 8;
    goto cleanup;
  }
  if (pread(image_fd, &before, 1U, 0) != 1) {
    result = 9;
    goto cleanup;
  }
  while (nanosleep(&settle, &settle) != 0 && errno == EINTR) {
  }
  if (pread(image_fd, &after, 1U, 0) != 1 || before != after) {
    result = 10;
    goto cleanup;
  }
  if (close(trace_pipe[0]) != 0) {
    result = 11;
    trace_pipe[0] = -1;
    goto cleanup;
  }
  trace_pipe[0] = -1;
#if defined(__linux__)
  if (formatter_test_descendant_group(image_fd) != 0) {
    result = 12;
    goto cleanup;
  }
  descendant_group_verified = 1;
#endif
  if (close(image_fd) != 0) {
    result = 13;
    image_fd = -1;
    goto cleanup;
  }
  image_fd = -1;
  if (formatter_test_exit_case(0, 23) != 0 ||
      formatter_test_exit_case(1, 0) != 0) {
    result = 14;
    goto cleanup;
  }
#if defined(__linux__) && defined(PR_SET_CHILD_SUBREAPER)
  if (formatter_test_parent_death() != 0) {
    result = 15;
    goto cleanup;
  }
  parent_death_verified = 1;
#endif
  if (printf("{\"descendantGroupStopped\":%s,"
             "\"execFailureReaped\":true,\"imageStable\":true,"
             "\"leaderOnlyMutationDetected\":%s,"
             "\"nonzeroReaped\":true,\"pipeClosed\":true,"
             "\"reaped\":true,\"parentDeathKill\":%s,"
             "\"termThenKill\":true}\n",
             descendant_group_verified ? "true" : "false",
             descendant_group_verified ? "true" : "false",
             parent_death_verified ? "true" : "false") < 0) {
    result = 16;
    goto cleanup;
  }
  result = 0;

cleanup:
  if (formatter_test_cleanup_child(&child, child_group_established) != 0 &&
      result == 0) {
    result = 17;
  }
  if (trace_pipe[0] >= 0) (void)close(trace_pipe[0]);
  if (trace_pipe[1] >= 0) (void)close(trace_pipe[1]);
  if (image_fd >= 0) (void)close(image_fd);
  if (!image_unlinked && image_fd >= 0) (void)unlink(image_path);
  return result;
}
