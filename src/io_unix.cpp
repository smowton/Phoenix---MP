
#include "stddefines.h"
#include "io_helpers.h"

char coord_dir[] = "/tmp/phoenix_coord_XXXXXX";
char fable_unix_buf[4096];
struct iovec vec;

void fable_init_unix() {

  if(!mkdtemp(coord_dir)) {
    fprintf(stderr, "Couldn't create a coordination directory: %s\n", strerror(errno));
    exit(1);
  }

}

void* fable_listen_unix(char* name) {

  int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  CHECK_ERROR((listen_fd == -1));

  setnb(listen_fd);

  struct sockaddr_un addr;
  addr.sun_family = AF_UNIX;
  int ret = snprintf(addr.sun_path, UNIX_PATH_MAX, "%s/mapper_%d_sock", coord_dir, threadid);
  CHECK_ERROR((ret >= 256));

  ret = bind(listen_fd, (struct sockaddr*)&addr, sizeof(struct sockaddr_un));
  CHECK_ERROR((ret == -1));

  ret = listen(listen_fd, 5);
  CHECK_ERROR((ret == -1));

  return (void*)listen_fd;

}

void* fable_accept_unix(void* listen_handle) {

  struct sockaddr_un otherend;
  socklen_t otherend_len = sizeof(otherend);
  int newfd = accept((int)listen_handle, (sockaddr*)&otherend, &otherend_len);
  if(newfd != -1)
    return (void*)newfd;
  else
    return 0;

}

void fable_set_nonblocking_unix(void* handle) {

  setnb((int)handle);

}

void fable_get_select_fds_unix(int type, void* handle, int* maxfd, fd_set* rfds, fd_set* wfds, fd_set* efds, struct timeval* timeout) {

  int fd = (int)handle;
  if(type == FABLE_SELECT_ACCEPT || type == FABLE_SELECT_READ)
    FD_SET(fd, &rfds);
  else
    FD_SET(fd, &wfds);
  if(fd >= *maxfd)
    *maxfd = (fd + 1);

}

int fable_ready_unix(int type, void* handle, fd_set* rfds, fd_set* wfds, fd_set* efds, struct timeval* timeout) {

  int fd = (int)handle;
  if(type == FABLE_SELECT_ACCEPT || type == FABLE_SELECT_READ)
    return FD_ISSET(fd, &rfds);
  else
    return FD_ISSET(fd, &wfds);

}

struct iovec* fable_get_write_buf_unix(void* handle, int len, int* nvecs) {

  vec.iov_buf = fable_unix_buf;
  vec.iov_len = len < 4096 ? len : 4096;
  *nvecs = 1;
  return &vec;

}

int fable_release_write_buf_unix(void* handle, struct iovec* buf, int nvecs) {

  

}
