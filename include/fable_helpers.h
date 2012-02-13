
#ifndef FABLE_HELPERS_H
#define FABLE_HELPERS_H

#ifdef FABLE_TYPE

#include <errno.h>
#include <sys/select.h>
#include <limits.h>

#include <ostream>

#include "stddefines.h"
#include "fable.h"

// Reads exactly len bytes or up to EOF.
void fable_read_all(void* handle, char* buf, int len) {

  int bytes_read = 0;
  while(bytes_read < len) {

    fd_set rfds;
    fd_set wfds;
    fd_set efds;
    FD_ZERO(&rfds);
    FD_ZERO(&wfds);
    FD_ZERO(&efds);
    struct timeval timeout;
    timeout.tv_sec = LONG_MAX;
    timeout.tv_usec = 0;  

    int maxfd = 0;

    fable_get_select_fds(handle, FABLE_SELECT_READ, &maxfd, &rfds, &wfds, &efds, &timeout);

    int selret = select(maxfd, &rfds, &wfds, &efds, &timeout);
    if(selret == -1) {
      CHECK_ERROR((errno != EAGAIN && errno != EINTR));
      continue;
    }

    if(fable_ready(handle, FABLE_SELECT_READ, &rfds, &wfds, &efds)) {
      
      int r = fable_lend_read_buf(handle, buf + bytes_read, len - bytes_read);
      if(r == -1 && (errno == EAGAIN || errno == EINTR))
	continue;
      CHECK_ERROR((r <= 0));
      bytes_read += r;

    }

  }

}

void fable_write_all(void* handle, const char* buf, int len) {

  struct fable_buf* fbuf = fable_lend_write_buf(handle, buf, len);
  CHECK_ERROR((!fbuf));

  while(1) {

    fd_set rfds;
    fd_set wfds;
    fd_set efds;
    FD_ZERO(&rfds);
    FD_ZERO(&wfds);
    FD_ZERO(&efds);
    struct timeval timeout;
    timeout.tv_sec = LONG_MAX;
    timeout.tv_usec = 0;  

    int maxfd = 0;

    fable_get_select_fds(handle, FABLE_SELECT_WRITE, &maxfd, &rfds, &wfds, &efds, &timeout);

    int selret = select(maxfd, &rfds, &wfds, &efds, &timeout);
    if(selret == -1) {
      CHECK_ERROR((errno != EAGAIN && errno != EINTR));
      continue;
    }

    if(fable_ready(handle, FABLE_SELECT_WRITE, &rfds, &wfds, &efds)) {

      int ret = fable_release_write_buf(handle, fbuf);
      if(ret == -1 && (errno == EAGAIN || errno == EINTR))
	continue;
      CHECK_ERROR((ret <= 0));
      // Otherwise, the buffer is free, we're done.
      return;

    }

  }

}

// Reads everything the handles have to give into their corresponding streams.
void fable_read_all_multi(void** handles, std::ostream** streams, unsigned int nstreams) {

  // Handles start all non-zero, connected for reading, non-blocking.

  unsigned conns_done = 0;
  while(conns_done < nstreams) {

    fd_set rfds;
    fd_set wfds;
    fd_set efds;
    FD_ZERO(&rfds);
    FD_ZERO(&wfds);
    FD_ZERO(&efds);
    struct timeval timeout;
    timeout.tv_sec = LONG_MAX;
    timeout.tv_usec = 0;

    int maxfd = 0;

    for(unsigned i = 0; i < nstreams; i++)
      if(handles[i])
	fable_get_select_fds(handles[i], FABLE_SELECT_READ, &maxfd, &rfds, &wfds, &efds, &timeout);

    int selret = select(maxfd, &rfds, &wfds, &efds, &timeout);
    if(selret == -1) {
      CHECK_ERROR((errno != EINTR && errno != EAGAIN));
      continue;
    }

    for(unsigned i = 0; i < nstreams; i++) {
      if(handles[i] && fable_ready(handles[i], FABLE_SELECT_READ, &rfds, &wfds, &efds)) {
	struct fable_buf* buf = fable_get_read_buf(handles[i], 4096);
	if(!buf) {
	  if(!errno) {
	    dprintf("Reducer %lu: mapper %u: EOF\n", id, i);
	    fable_close(handles[i]);
	    handles[i] = 0;
	    conns_done++;
	  }
	  else
	    CHECK_ERROR((errno != EAGAIN && errno != EINTR));
	  continue;
	}
	else {
	  for(int j = 0; j < buf->nbufs; ++j)
	    streams[i]->write((const char*)buf->bufs[j].iov_base, buf->bufs[j].iov_len);
	  fable_release_read_buf(handles[i], buf);
	}
      }
    }

  }

}

#endif

#endif
