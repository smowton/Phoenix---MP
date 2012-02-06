
#ifndef FABLE_H
#define FABLE_H

struct fable_buf {

  struct iovec* bufs;
  int nbufs;
  int written;

} __attribute__((packed));

#ifndef FABLE_TYPE
#error "Must define FABLE_TYPE to use the Fable prototype. Available types: unix"
#endif

#define fable_init fable_init_ ## FABLE_TYPE
#define fable_connect fable_connect_ ## FABLE_TYPE
#define fable_listen fable_listen_ ## FABLE_TYPE
#define fable_accept fable_accept_ ## FABLE_TYPE
#define fable_set_nonblocking fable_set_nonblocking_ ## FABLE_TYPE
#define fable_get_select_fds fable_get_select_fds_ ## FABLE_TYPE
#define fable_ready fable_ready_ ## FABLE_TYPE
#define fable_get_write_buf fable_get_write_buf_ ## FABLE_TYPE
#define fable_lend_write_buf fable_lend_write_buf_ ## FABLE_TYPE
#define fable_release_write_buf fable_release_write_buf_ ## FABLE_TYPE
#define fable_get_read_buf fable_get_read_buf_ ## FABLE_TYPE
#define fable_lend_read_buf fable_lend_read_buf_ ## FABLE_TYPE
#define fable_release_read_buf fable_release_read_buf_ ## FABLE_TYPE

#define FABLE_SELECT_READ 1
#define FABLE_SELECT_WRITE 2
#define FABLE_SELECT_ACCEPT 3

void fable_init_unix();
void* fable_connect_unix(const char* name);
void* fable_listen_unix(const char* name);
void* fable_accept_unix(void* listen_handle);
void fable_set_nonblocking_unix(void* handle);
void fable_get_select_fds_unix(void* handle, int type, int* maxfd, fd_set* rfds, fd_set* wfds, fd_set* efds, struct timeval* timeout);
int fable_ready_unix(void* handle, int type fd_set* rfds, fd_set* wfds, fd_set* efds, struct timeval* timeout);
struct fable_buf* fable_get_write_buf_unix(void* handle, int len);
struct fable_buf* fable_lend_write_buf_unix(void* handle, char* buf, int len);
int fable_release_write_buf_unix(void* handle, struct fable_buf* buf);
int fable_lend_read_buf(void* handle, char* buf, int len);
struct fable_buf* fable_get_read_buf(void* handle, int len);
void fable_release_read_buf(void* handle, struct fable_buf* buf);
void fable_close_unix(void* handle);

#endif
