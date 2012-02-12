
#include "stddefines.h"
#include "io_helpers.h"
#include "fable.h"

#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include <sys/socket.h>
#include <linux/un.h> // For UNIX_PATH_MAX

char coord_dir[] = "/tmp/phoenix_coord_XXXXXX";
char fable_unix_buf[4096];

#define MAX_UNIX_BUF (4096 - (sizeof(struct fable_buf) + sizeof(struct iovec)))

struct fable_buf_unix {

  struct fable_buf base;
  struct iovec unix_vec;

} __attribute__((packed));

void fable_init_unixdomain() {

  if(!mkdtemp(coord_dir)) {
    fprintf(stderr, "Couldn't create a coordination directory: %s\n", strerror(errno));
    exit(1);
  }

}

// Ignore direction, all Unix sockets are duplex
void* fable_connect_unixdomain(const char* name, int direction) {

  struct sockaddr_un addr;
  addr.sun_family = AF_UNIX;
  int ret = snprintf(addr.sun_path, UNIX_PATH_MAX, "%s/%s", coord_dir, name);
  if(ret >= UNIX_PATH_MAX) {
    errno = ENAMETOOLONG;
    return 0;
  }
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if(fd == -1)
    return 0;

  ret = connect(fd, (struct sockaddr*)&addr, sizeof(struct sockaddr_un));
  if(ret == -1) {
    close(fd);
    return 0;
  }

  int* handle = (int*)malloc(sizeof(int));
  *handle = fd;
  return handle;

}

void* fable_listen_unixdomain(const char* name) {

  int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if(listen_fd == -1)
    return 0;

  struct sockaddr_un addr;
  addr.sun_family = AF_UNIX;
  int ret = snprintf(addr.sun_path, UNIX_PATH_MAX, "%s/%s", coord_dir, name);
  if(ret >= 256) {
    errno = ENAMETOOLONG;
    return 0;
  }

  ret = bind(listen_fd, (struct sockaddr*)&addr, sizeof(struct sockaddr_un));
  if(ret == -1)
    return 0;

  ret = listen(listen_fd, 5);
  if(ret == -1)
    return 0;

  int* handle = (int*)malloc(sizeof(int));
  *handle = listen_fd;
  return handle;

}

// Ignore direction, all Unix sockets are duplex
void* fable_accept_unixdomain(void* listen_handle, int direction) {

  struct sockaddr_un otherend;
  socklen_t otherend_len = sizeof(otherend);
  int newfd = accept(*((int*)listen_handle), (sockaddr*)&otherend, &otherend_len);
  if(newfd != -1) {
    int* handle = (int*)malloc(sizeof(int));
    *handle = newfd;
    return handle;
  }
  else
    return 0;

}

void fable_set_nonblocking_unixdomain(void* handle) {

  setnb_fd(*((int*)handle));

}

void fable_get_select_fds_unixdomain(void* handle, int type, int* maxfd, fd_set* rfds, fd_set* wfds, fd_set* efds, struct timeval* timeout) {

  int fd = *((int*)handle);
  if(type == FABLE_SELECT_ACCEPT || type == FABLE_SELECT_READ)
    FD_SET(fd, rfds);
  else
    FD_SET(fd, wfds);
  if(fd >= *maxfd)
    *maxfd = (fd + 1);

}

int fable_ready_unixdomain(void* handle, int type, fd_set* rfds, fd_set* wfds, fd_set* efds) {

  int fd = *((int*)handle);
  if(type == FABLE_SELECT_ACCEPT || type == FABLE_SELECT_READ)
    return FD_ISSET(fd, rfds);
  else
    return FD_ISSET(fd, wfds);

}

struct fable_buf* fable_get_write_buf_unixdomain(void* handle, unsigned len) {

  int malloc_sz = sizeof(struct fable_buf_unix) + len;
  if(malloc_sz > 4096) {
    len -= (malloc_sz - 4096);
    malloc_sz = 4096;
  }
  
  struct fable_buf_unix* new_buf = (struct fable_buf_unix*)malloc(malloc_sz);
  new_buf->base.bufs = &new_buf->unix_vec;
  new_buf->base.nbufs = 1;
  new_buf->base.written = 0;
  new_buf->unix_vec.iov_len = len;
  new_buf->unix_vec.iov_base = (char*)&(new_buf[1]);

  return &(new_buf->base);

}

struct fable_buf* fable_lend_write_buf_unixdomain(void* handle, const char* buf, unsigned len) {

  struct fable_buf_unix* new_buf = (struct fable_buf_unix*)malloc(sizeof(struct fable_buf_unix));
  new_buf->base.bufs = &new_buf->unix_vec;
  new_buf->base.nbufs = 1;
  new_buf->base.written = 0;
  new_buf->unix_vec.iov_base = (void*)buf;
  new_buf->unix_vec.iov_len = len;
  
  return &(new_buf->base);

}

int fable_release_write_buf_unixdomain(void* handle, struct fable_buf* buf) {

  int fd = *((int*)handle);

  int remaining_write = buf->bufs[0].iov_len - buf->written;
  int write_ret = write(fd, ((char*)buf->bufs[0].iov_base) + buf->written, remaining_write);

  // Freeing buf should be safe as it's the first member of struct fable_buf_unix.
  if(write_ret == -1) {
    if(errno == EAGAIN || errno == EINTR) {
      errno = EAGAIN;
      return -1;
    }
    else {
      free(buf);
      return -1;
    }
  }
  else if(write_ret == 0) {
    free(buf);
    return 0;
  }
  else if(write_ret < remaining_write) {
    errno = EAGAIN;
    buf->written += write_ret;
    return -1;
  }
  else {
    free(buf);
    return 1;
  }

}

int fable_lend_read_buf_unixdomain(void* handle, char* buf, unsigned len) {

  int fd = *((int*)handle);
  return read(fd, buf, len);

}

struct fable_buf* fable_get_read_buf_unixdomain(void* handle, unsigned len) {

  int malloc_sz = sizeof(struct fable_buf_unix) + len;
  if(malloc_sz > 4096) {
    len -= (malloc_sz - 4096);
    malloc_sz = 4096;
  }

  struct fable_buf_unix* ret = (struct fable_buf_unix*)malloc(malloc_sz);
  ret->base.written = 0;
  ret->base.nbufs = 1;
  ret->base.bufs = &ret->unix_vec;

  ret->unix_vec.iov_base = &(ret[1]);
  
  int fd = *((int*)handle);
  int this_read = read(fd, ret->unix_vec.iov_base, len);
  if(this_read <= 0) {
    free(ret);
    if(this_read == 0)
      errno = 0;
    return 0;
  }
  else if(this_read < len) {
    ret = (struct fable_buf_unix*)realloc(ret, sizeof(struct fable_buf_unix) + this_read);
    ret->unix_vec.iov_base = &(ret[1]);
  }
  ret->unix_vec.iov_len = this_read;
  
  return &(ret->base);

}

void fable_release_read_buf_unixdomain(void* handle, struct fable_buf* buf) {

  free(buf); // == address of 'superstruct' fable_buf_unix

}

void fable_close_unixdomain(void* handle) {

  close(*((int*)handle));
  free(handle);

}
