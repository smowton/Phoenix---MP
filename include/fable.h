
#ifndef FABLE_H
#define FABLE_H

struct fable_buf {

  struct iovec* bufs;
  int nbufs;
  int written;

} __attribute__((packed));

#endif
