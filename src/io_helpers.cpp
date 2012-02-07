
#include <sys/select.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

#include "io_helpers.h"

void read_all_fd(int fd, char* buf, int len) {

  fd_set rfds;

  while(len) {

    FD_ZERO(&rfds);
    FD_SET(fd, &rfds);

    int ret = select(fd + 1, &rfds, 0, 0, 0);

    if(ret == -1) {
      if(errno == EINTR)
	continue;
      fprintf(stderr, "Select failed: %s\n", strerror(errno));
      exit(1);
    }
    else if(ret == 0) {
      fprintf(stderr, "Unexpected EOF reading\n");
      exit(1);
    }

    int this_read = read(fd, buf, len);
    if(this_read == -1) {
      if(errno == EINTR || errno == EAGAIN)
	continue;
      fprintf(stderr, "Read failed: %s\n", strerror(errno));
      exit(1);
    }

    len -= this_read;
    buf += this_read;

  }

}

void write_all_fd(int fd, const char* buf, int len) {

  fd_set wfds;

  while(len) {
    
    FD_ZERO(&wfds);
    FD_SET(fd, &wfds);

    int ret = select(fd + 1, 0, &wfds, 0, 0);

    if(ret == -1) {
      if(errno == EAGAIN || errno == EINTR)
	continue;
      fprintf(stderr, "Select failed: %s\n", strerror(errno));
      exit(1);
    }
    else if(ret == 0) {
      fprintf(stderr, "Unexpected EOF writing\n");
      exit(1);
    }

    int this_write = write(fd, buf, len);
    if(this_write == -1) {
      if(errno == EAGAIN || errno == EINTR)
	continue;
      fprintf(stderr, "Write failed: %s\n", strerror(errno));
      exit(1);
    }

    len -= this_write;
    buf += this_write;

  }

}

void setnb_fd(int fd) {

  int flags = fcntl(fd, F_GETFL);
  flags |= O_NONBLOCK;
  fcntl(fd, F_SETFL, flags);

}
