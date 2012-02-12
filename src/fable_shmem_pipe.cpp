/* Another kind of shared memory test.  The idea here is that we have
   a shared-memory region and a malloc-like thing for allocating from
   it, and we also have a couple of pipes.  Messages are sent by
   allocating a chunk of the shared region and then sending an extent
   through the pipe.  Once the receiver is finished with the message,
   they send another extent back through the other pipe saying that
   they're done. */
#include <sys/poll.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <assert.h>
#include <err.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define PAGE_ORDER 12
#define CACHE_LINE_SIZE 64
static unsigned ring_order = 9;
#define ring_size (1ul << (PAGE_ORDER + ring_order))

#define EXTENT_BUFFER_SIZE 4096

#define ALLOC_FAILED ((unsigned)-1)

#ifdef NDEBUG
#define DBG(x) do {} while (0)
#else
#define DBG(x) do { x; } while (0)
#endif

struct extent {
	unsigned base;
	unsigned size;
};

// To differentiate the two handle types in close(); all other callbacks only accept one subtype.
#define SHMEMPIPE_HANDLE_LISTEN
#define SHMEMPIPE_HANDLE_CONN

struct shmem_pipe_handle {

  int handle_type;
  int fd;

};

struct shmem_pipe_handle_conn {

        struct shmem_pipe_handle base;

        int direction;
	void *ring;
	struct alloc_node *first_alloc, *next_free_alloc, *last_freed_node;
#ifndef NDEBUG
	int nr_alloc_nodes;
#endif

  // Parent state
	unsigned char rx_buf[EXTENT_BUFFER_SIZE];
	unsigned rx_buf_prod;
	unsigned rx_buf_cons;

  // Child state
	unsigned char incoming[EXTENT_BUFFER_SIZE];
	struct extent outgoing_extents[EXTENT_BUFFER_SIZE/sizeof(struct extent)];
        int incoming_bytes;
        int incoming_bytes_consumed;
        unsigned nr_outgoing_extents;
	unsigned outgoing_extent_bytes;

};

struct shmem_pipe_buf {

  struct fable_buf base;
  struct iovec iov;

};

/* Our allocation structure is a simple linked list.  That's pretty
   stupid, *except* that the allocation pattern is almost always a
   very simple queue, so it becomes very simple.  i.e. we release
   stuff in a FIFO order wrt allocations, so we effectively just have
   one allocated region which loops around the shared area, which
   makes the linked list very short and everything is very easy. */
struct alloc_node {
	struct alloc_node *next, *prev;
	int is_free;
	unsigned long start;
	unsigned long end;
};

#ifndef NDEBUG
static void
sanity_check(const struct shmem_pipe *sp)
{
	const struct alloc_node *cursor;
	int found_nf = 0, found_lf = 0;
	int n = 0;
	assert(sp->first_alloc);
	assert(sp->first_alloc->start == 0);
	for (cursor = sp->first_alloc;
	     cursor;
	     cursor = cursor->next) {
		n++;
		if (cursor == sp->first_alloc)
			assert(!cursor->prev);
		else
			assert(cursor->prev);
		if (cursor->next)
			assert(cursor->next->prev == cursor);
		if (cursor->prev)
			assert(cursor->start == cursor->prev->end);
		if (cursor->prev)
			assert(cursor->is_free == !cursor->prev->is_free);
		if (!cursor->next)
			assert(cursor->end == ring_size);
		if (cursor == sp->next_free_alloc) {
			assert(!found_nf);
			found_nf = 1;
		}
		if (cursor == sp->last_freed_node) {
			assert(!found_lf);
			found_lf = 1;
		}
		assert(cursor->start < cursor->end);
	}
	if (!found_nf)
		assert(!sp->next_free_alloc);
	else
		assert(sp->next_free_alloc->is_free);
	if (!found_lf)
		assert(!sp->last_freed_node);
	else
		assert(sp->last_freed_node->is_free);
	assert(n == sp->nr_alloc_nodes);
}
#else
static void
sanity_check(const struct shmem_pipe *sp)
{
}
#endif

static unsigned any_shared_space(struct shmem_pipe *sp)
{

  /* As its name suggests, if next_free_alloc is set, then it points to a free area. We can write at least 1 byte, so we're writable. */
  if(sp->next_free_alloc)
    return 1;
  /* Search the linked list. Don't keep next_free_alloc as it'd make the alloc_shared take a silly decision when we stop at a 1-byte hole.  */

  struct alloc_node* n;
  for (n = sp->first_alloc; n && (!n->is_free); n = n->next) ;

  return (n != 0);

}

static unsigned
alloc_shared_space(struct shmem_pipe *sp, unsigned* size_out)
{
        unsigned size = *size_out;
	unsigned res;

	sanity_check(sp);

	/* Common case */
	if (sp->next_free_alloc &&
	    sp->next_free_alloc->end >= size + sp->next_free_alloc->start &&
	    sp->next_free_alloc->prev) {
	allocate_next_free:
		assert(!sp->next_free_alloc->prev->is_free);
		assert(sp->next_free_alloc->is_free);
		res = sp->next_free_alloc->start;
		sp->next_free_alloc->start += size;
		sp->next_free_alloc->prev->end += size;
		if (sp->next_free_alloc->start == sp->next_free_alloc->end) {
			if (sp->next_free_alloc->next) {
				assert(!sp->next_free_alloc->next->is_free);
				sp->next_free_alloc->prev->next = sp->next_free_alloc->next->next;
				sp->next_free_alloc->prev->end = sp->next_free_alloc->next->end;
				if (sp->next_free_alloc->next->next) {
					assert(sp->next_free_alloc->next->next->is_free);
					sp->next_free_alloc->next->next->prev = sp->next_free_alloc->prev;
				}
				struct alloc_node *p = sp->next_free_alloc->next->next;
				DBG(sp->nr_alloc_nodes--);
				free(sp->next_free_alloc->next);
				if (sp->next_free_alloc->next == sp->last_freed_node)
					sp->last_freed_node = NULL;
				sp->next_free_alloc->next = p;
			} else {
				if (sp->next_free_alloc->prev)
					sp->next_free_alloc->prev->next = NULL;
			}
			if (sp->first_alloc == sp->next_free_alloc) {
				assert(sp->next_free_alloc->next);
				assert(!sp->next_free_alloc->prev);
				sp->first_alloc = sp->next_free_alloc->next;
			}
			if (sp->next_free_alloc == sp->last_freed_node)
				sp->last_freed_node = NULL;
			DBG(sp->nr_alloc_nodes--);
			free(sp->next_free_alloc);
			sp->next_free_alloc = NULL;
		}
		sanity_check(sp);
		return res;
	}

	struct alloc_node *best_candidate = 0;
	unsigned int best_size = 0;

	/* Slightly harder case: have to search the linked list */
	for (sp->next_free_alloc = sp->first_alloc;
	     sp->next_free_alloc &&
		     (!sp->next_free_alloc->is_free || sp->next_free_alloc->end - sp->next_free_alloc->start < size);
	     sp->next_free_alloc = sp->next_free_alloc->next) {
	         
	  /* sp->next_free_alloc isn't enough or doesn't exist, but keep track of the next-best option */
	  if(sp->next_free_alloc && sp->next_free_alloc->is_free) {
	    unsigned int this_size = sp->next_free_alloc->end - sp->next_free_alloc->start;
	    if(this_size > best_size) {
	      best_size = this_size;
	      best_candidate = sp->next_free_alloc;
	    }
	  }

	}
	if (!sp->next_free_alloc) {
		/* Shared area has no gaps large enough, but in order to behave like a selectable device we
		   must return the next best candidate if there is one. */
	  if(best_candidate) {
	    sp->next_free_alloc = best_candidate;
	    (*out_size) = size = best_size;
	  }
	  else
	    return ALLOC_FAILED;
	}

	struct alloc_node *f = sp->next_free_alloc;
	assert(f->is_free);
	if (!f->prev) {
		/* Allocate the start of the arena. */
		assert(f->start == 0);
		assert(f == sp->first_alloc);
		if (f->end == size) {
			/* We're going to convert next_free_alloc to
			 * an in-use node.  This may involve forwards
			 * merging. */
			if (f->next) {
				struct alloc_node *t = f->next;
				assert(!t->is_free);
				f->end = t->end;
				f->next = t->next;
				if (f->next)
					f->next->prev = f;
				if (sp->last_freed_node == t)
					sp->last_freed_node = NULL;
				DBG(sp->nr_alloc_nodes--);
				free(t);
			}
			f->is_free = 0;
		} else {
			f = calloc(sizeof(struct alloc_node), 1);
			DBG(sp->nr_alloc_nodes++);
			f->next = sp->first_alloc;
			f->start = 0;
			f->end = size;
			assert(f->next);
			f->next->prev = f;
			f->next->start = size;
			sp->first_alloc = f;
		}
		if (sp->last_freed_node == sp->first_alloc)
			sp->last_freed_node = sp->first_alloc->next;
		if (sp->next_free_alloc == sp->first_alloc)
			sp->next_free_alloc = sp->first_alloc->next;
		sanity_check(sp);
		return 0;
	} else {
		goto allocate_next_free;
	}
}

static void
release_shared_space(struct shmem_pipe *sp, unsigned start, unsigned size)
{
	struct alloc_node *lan = sp->last_freed_node;
	assert(start <= ring_size);
	assert(start + size <= ring_size);
	assert(size > 0);
	sanity_check(sp);
	if (lan &&
	    lan->is_free &&
	    lan->end == start) {
		struct alloc_node *next;
	free_from_here:
		next = lan->next;
		assert(next);
		assert(!next->is_free);
		assert(next->start == start);
		assert(next->end >= start + size);
		next->start += size;
		lan->end += size;
		if (next->start == next->end) {
			/* We just closed a hole.  Previously, we had
			   LAN->next->X, where LAN is sp->last_freed_node,
			   next is some free region, and X is either
			   NULL or some allocated region.  next is now
			   zero-sized, so we want to remove it and
			   convert to LAN->X.  However, LAN and X are
			   the same type (i.e. both non-free), so we
			   can extend LAN to cover X and remove X as
			   well. */
			struct alloc_node *X = next->next;

			if (X) {
				/* Convert LAN->next->X->Y into
				   LAN->Y */
				struct alloc_node *Y = X->next;
				assert(X->is_free);
				if (Y) {
					assert(!Y->is_free);
					Y->prev = lan;
				}
				lan->end = X->end;
				lan->next = Y;
				if (X == sp->next_free_alloc)
					sp->next_free_alloc = lan;
				DBG(sp->nr_alloc_nodes--);
				free(X);
			} else {
				/* Just turn LAN->free1->NULL into
				   LAN->NULL */
				assert(lan->end == next->start);
				lan->next = NULL;
			}
			if (next == sp->next_free_alloc)
				sp->next_free_alloc = lan;
			DBG(sp->nr_alloc_nodes--);
			free(next);
		}
		sanity_check(sp);
		return;
	}

	/* More tricky case: we're freeing something which doesn't hit
	 * the cache. */
	for (lan = sp->first_alloc;
	     lan && (lan->end <= start || lan->start > start);
	     lan = lan->next)
		;
	assert(lan); /* Or else we're freeing something outside of the arena */
	assert(!lan->is_free); /* Or we have a double free */
	if (lan->start == start) {
		/* Free out the start of this block. */
		assert(!lan->is_free);
		if (lan->prev) {
			assert(lan->prev->is_free);
			assert(lan->prev->end == start);
			sp->last_freed_node = lan = lan->prev;
			goto free_from_here;
		}
		/* Turn the very start of the arena into a free
		 * block */
		assert(lan == sp->first_alloc);
		assert(start == 0);
		if (lan->end == size) {
			/* Easy: just convert the existing node to a
			 * free one. */
			lan->is_free = 1;
			if (lan->next && lan->next->is_free) {
				/* First node is now free, and the
				   second node already was -> merge
				   them. */
				struct alloc_node *t = lan->next;
				lan->end = t->end;
				lan->next = t->next;
				if (lan->next)
					lan->next->prev = lan;
				if (sp->last_freed_node == t)
					sp->last_freed_node = lan;
				if (sp->next_free_alloc == t)
					sp->next_free_alloc = lan;
				DBG(sp->nr_alloc_nodes--);
				free(t);
			}
			sanity_check(sp);
		} else {
			/* Need a new node in the list */
			lan = calloc(sizeof(*lan), 1);
			lan->is_free = 1;
			lan->end = size;
			sp->first_alloc->start = lan->end;
			sp->first_alloc->prev = lan;
			lan->next = sp->first_alloc;
			sp->first_alloc = lan;
			sp->last_freed_node = sp->first_alloc;
			DBG(sp->nr_alloc_nodes++);
			sanity_check(sp);
		}
		return;
	}
	assert(start < lan->end);
	assert(start + size <= lan->end);
	if (start + size == lan->end) {
		/* Free out the end of this block */
		if (lan->next) {
			assert(lan->next->is_free);
			lan->next->start -= size;
			lan->end -= size;
			assert(lan->end != lan->start);
		} else {
			struct alloc_node *t = calloc(sizeof(*lan), 1);
			t->prev = lan;
			t->is_free = 1;
			t->start = start;
			t->end = start + size;
			lan->next = t;
			lan->end = start;
			DBG(sp->nr_alloc_nodes++);
		}
		if (!sp->next_free_alloc)
			sp->next_free_alloc = lan->next;
		sp->last_freed_node = lan->next;
		sanity_check(sp);
		return;
	}

	/* Okay, this is the tricky case.  We have a single allocated
	   node, and we need to convert it into three: an allocated
	   node, a free node, and then another allocated node.  How
	   tedious. */
	struct alloc_node *a = calloc(sizeof(*a), 1);
	struct alloc_node *b = calloc(sizeof(*b), 1);

	a->next = b;
	a->prev = lan;
	a->is_free = 1;
	a->start = start;
	a->end = start + size;

	b->next = lan->next;
	b->prev = a;
	b->is_free = 0;
	b->start = start + size;
	b->end = lan->end;

	if (lan->next)
		lan->next->prev = b;
	lan->next = a;
	lan->end = start;

	DBG(sp->nr_alloc_nodes += 2);

	if (!sp->next_free_alloc)
		sp->next_free_alloc = a;

	/* And we're done. */
	sanity_check(sp);
}

void
fable_init_shmem_pipe()
{
	char *_ring_order = getenv("SHMEM_RING_ORDER");
	if (_ring_order) {
		int as_int = -1;
		if (sscanf(_ring_order, "%d", &as_int) != 1)
		  fprintf(stderr, "SHMEM_RING_ORDER must be an integer; ignored");
		else {
		  if (as_int < 0 || as_int > 15)
		    fprintf(stderr, "SHMEM_RING_ORDER must be between 0 and 15; ignored");
		  else
		    ring_order = as_int;
		}
	}

	fable_init_unixdomain();
}

void*
fable_listen_shmem_pipe(char* name) {
  
  void* unix_handle = fable_listen_unixdomain(name);
  if(!unix_handle)
    return 0;
  struct shmem_pipe_handle* new_handle = (struct shmem_pipe_handle*)malloc(sizeof(struct shmem_pipe_handle));
  if(!new_handle) {
    errno = ENOMEM;
    return 0;
  }

  new_handle->type = SHMEMPIPE_HANDLE_LISTEN;
  new_handle->fd = *((int*)unix_handle);
  free(unix_handle);
  return new_handle;

}

void shmem_pipe_init_recv(struct shmem_pipe_handle_conn* new_handle) {

  new_handle->first_alloc = calloc(sizeof(*sp->first_alloc), 1);
  new_handle->first_alloc->is_free = 1;
  new_handle>first_alloc->end = ring_size;
  DBG(new_handle->nr_alloc_nodes = 1);

}

void*
fable_accept_shmem_pipe(void* handle, int direction) {

  if(direction == FABLE_DIRECTION_DUPLEX) {
    fprintf(stderr, "shmem_pipe transport can't do duplex yet\n");
    return 0;
  }

  struct shmem_pipe_handle* listen_handle = (struct shmem_pipe_handle*)handle;
  
  void* new_unix_handle = fable_accept_unixdomain(&listen_handle->fd, FABLE_DIRECTION_DUPLEX);
  if(!new_unix_handle)
    return 0;

  int conn_fd = *((int*)new_unix_handle);
  free(new_unix_handle);

  // TODO: these are blocking operations, even if the Fable handle is nonblocking.
  int shmem_fd = unix_recv_fd(conn_fd);
  if(shmem_fd == -1) {
    close(conn_fd);
    return 0;
  }

  int seg_pages;
  fd_recv_all(conn_fd, &seg_pages, sizeof(int));

  void* ring_addr = mmap(NULL, PAGE_SIZE * nr_pages, PROT_READ|PROT_WRITE, MAP_SHARED, shmem_fd, 0);
  if(ring_addr == MAP_FAILED) {
    close(shmem_fd);
    close(conn_fd);
    return 0;
  }

  struct shmem_pipe_handle_conn* new_handle = (struct shmem_pipe_handle_conn*)malloc(sizeof(struct shmem_pipe_handle_conn));
  if(!new_handle) {
    errno = ENOMEM;
    close(shmem_fd);
    close(conn_fd);
    return 0;
  }

  memset(new_handle, 0, sizeof(struct shmem_pipe_handle_conn));
  new_handle->base.fd = conn_fd;
  new_handle->base.type = SHMEMPIPE_HANDLE_CONN;
  new_handle->ring = ring_addr;
  new_handle->direction = direction;

  if(direction == FABLE_DIRECTION_RECV)
    shmem_pipe_init_recv(new_handle);
  // Otherwise memset-0 is enough

  return new_handle;
  
}

void* fable_connect_shmem_pipe(char* name, int direction) {

  // As the connection initiator, it's our responsibility to supply shared memory.

  char random_name[21];
  int ring_pages = pow(2, ring_size);
  
  int shm_fd = -1;
  errno = EEXIST;
  for(int i = 0; i < 100 && shm_fd == -1 && errno == EEXIST; ++i) {
    strcpy(random_name, "/fable_segment_XXXXXX");
    mktemp(random_name);
    shm_fd = shm_open(random_name, O_RDWR|O_CREAT|O_EXCL, 0600);
  }

  if(shm_fd == -1)
    return 0;

  shm_unlink(random_name);
  if (ftruncate(shm_fd, PAGE_SIZE * ring_pages) < 0) {
    close(shm_fd);
    return 0;
  }

  void* ring_addr = mmap(NULL, PAGE_SIZE * ring_pages, PROT_READ|PROT_WRITE, MAP_SHARED, shm_fd, 0);
 
  void* unix_handle = fable_connect_unixdomain(name, FABLE_DIRECTION_DUPLEX);
  if(!unix_handle)
    return 0;

  int unix_fd = *((int*)unix_handle);
  free(unix_handle);

  // OK, send our partner the FD and the appropriate size to mmap.
  int send_ret = unix_send_fd(unix_fd, shm_fd);
  close(shm_fd);
  
  if(send_ret <= 0) {
    munmap(ring_addr, PAGE_SIZE * ring_pages);
    close(unix_fd);
    return 0;
  }

  send_all_fd(unix_fd, &ring_pages, sizeof(int));

  struct shmem_pipe_handle_conn* conn_handle = (struct shmem_pipe_handle_conn*)malloc(sizeof(struct shmem_pipe_hamdle_conn));
  memset(conn_handle, 0, sizeof(struct shmem_pipe_handle_conn));

  conn_handle->base.fd = unix_fd;
  conn_handle->base.type = SHMEMPIPE_HANDLE_CONN;
  conn_handle->ring = ring_addr;
  conn_handle->direction = direction;

  if(direction == FABLE_DIRECTION_RECV)
    shmem_pipe_init_recv(new_handle);
  // Otherwise memset-0 is enough

  return conn_handle;

}

struct fable_buf* fable_get_read_buf_shmem_pipe(void* handle, int len) {

  struct shmem_pipe_handle_conn *sp = (struct shmem_pipe_handle_conn*)handle;

  while(sp->incoming_bytes_consumed - sp->incoming_bytes < sizeof(struct extent)) {

    int k = read(sp->fd, (void *)sp->incoming + sp->incoming_bytes, sizeof(sp->incoming) - sp->incoming_bytes);
    if (k == 0) {
      errno = 0;
      return 0;
    }
    if (k < 0) {
      return 0;
    }
    sp->incoming_bytes += k;

  }
  
  struct extent *inc = (struct extent*)(sp->incoming + sp->incoming_bytes_consumed);

  struct shmem_pipe_buf* buf = (struct shmem_pipe_buf*)malloc(sizeof(struct shmem_pipe_buf));
  buf->iov.iov_base = sp->ring + inc->base;
  buf->iov.iov_len = std::min(inc->size, len);
  buf->base.bufs = &buf->iov;
  buf->base.nbufs = 1;
  return &buf->base;

}

static void release_read_buffer(void* handle, struct fable_buf* fbuf) {

  struct shmem_pipe_handle_conn *sp = (struct shmem_pipe_handle_conn*)handle;
  struct shmem_pipe_buf* buf = (struct shmem_pipe_buf*)fbuf;

  assert(buf->base.nbufs == 1 && buf->base.bufs == &buf->iov);

  struct extent *inc = (struct extent*)(sp->incoming + sp->incoming_bytes_consumed);
  assert(sp->ring + inc->base == fbuf->bufs[0].iov_base);
  
  struct extent tosend;
  // Was the extent fully consumed by this read?
  if(fbuf->bufs[0].iov_len == inc->size) {

    tosend = *inc;

    // Dismiss this incoming extent
    sp->incoming_bytes_consumed += sizeof(struct extent);

    if(sp->incoming_bytes_consumed - sp->incoming_bytes < sizeof(struct extent)) {
      memmove(sp->incoming, sp->incoming + sp->incoming_bytes_consumed, sp->incoming_bytes - sp->incoming_bytes_consumed);
      sp->incoming_bytes -= sp->incoming_bytes_consumed;
      sp->incoming_bytes_consumed = 0;
    }

  }
  // Otherwise, divide the extent in two; queue one and keep the other.
  else {
    
    tosend.base = inc->base;
    tosend.size = fbuf->bufs[0].iov_len;
    inc->base += fbuf->bufs[0].iov_len;
    inc->size -= fbuf->bufs[0].iov_len;

  }

  // Queue it for transmission back to the writer
  struct extent *out;
  out = &sp->outgoing_extents[sp->nr_outgoing_extents-1];
  /* Try to reuse previous outgoing extent */
  if (sp->nr_outgoing_extents != 0 && out->base + out->size == to_send.base) {
    out->size += to_send.size;
  } else {
    sp->outgoing_extents[sp->nr_outgoing_extents] = to_send; // Struct copy
    sp->nr_outgoing_extents++;
  }
  sp->outgoing_extent_bytes += to_send.size;

  // Send the queued extents, if the queue is big enough

  // TODO: blocking operations regardless of nonblocking status.
  if (sp->outgoing_extent_bytes > ring_size / 8) {
    fd_send_all(sp->base.fd, sp->outgoing_extents, sp->nr_outgoing_extents * sizeof(struct extent));
    sp->nr_outgoing_extents = 0;
    sp->outgoing_extent_bytes = 0;
  }

  free(fbuf);

}

// Return 1 for success, 0 for remote closed, -1 for error including EAGAIN
int wait_for_returned_buffers(struct shmem_pipe_handle_conn *sp)
{
	int r;
	int s;
	static int total_read;

	s = read(sp->base.fd, sp->rx_buf + sp->rx_buf_prod, sizeof(sp->rx_buf) - sp->rx_buf_prod);
	if (s <= 0)
	  return s;
	total_read += s;
	sp->rx_buf_prod += s;
	for (r = 0; r < sp->rx_buf_prod / sizeof(struct extent); r++) {
		struct extent *e = &((struct extent *)sp->rx_buf)[r];
		release_shared_space(sp, e->base, e->size);
	}
	if (sp->rx_buf_prod != r * sizeof(struct extent))
		memmove(sp->rx_buf,
			sp->rx_buf + sp->rx_buf_prod - (sp->rx_buf_prod % sizeof(struct extent)),
			sp->rx_buf_prod % sizeof(struct extent));
	sp->rx_buf_prod %= sizeof(struct extent);
	return 1;
}

struct fable_buf* fable_get_write_buf_shmem_pipe(void* handle, unsigned len)
{
  struct shmem_pipe_handle_conn *sp = (struct shmem_pipe_handle_conn*)handle;
  unsigned long offset;
  unsigned allocated_len = len;

  while ((offset = alloc_shared_space(sp, &allocated_len)) == ALLOC_FAILED) {
    int wait_ret = wait_for_returned_buffers(sp);
    if(wait_ret == 0)
      errno = 0;
    if(wait_ret <= 0)
      return 0;
  }

  struct shmem_pipe_buf* buf = (struct shmem_pipe_buf*)malloc(sizeof(struct shmem_pipe_buf));

  buf->iov.iov_base = sp->ring + offset;
  buf->iov.iov_len = allocated_len;
  buf->base.bufs = &buf->iov;
  buf->base.nbufs = 1;

  return &buf->base;
}

int fable_release_write_buf_shmem_pipe(void* handle, struct fable_buf* fbuf)
{
  struct shmem_pipe_handle_conn *sp = (struct shmem_pipe_handle_conn*)handle;
  struct shmem_pipe_buf *buf = (struct shmem_pipe_buf*)fbuf;
  struct extent ext;

  assert(fbuf->nbufs == 1 && fbuf->bufs[0] == &buf->iov);

  unsigned long offset = fbuf->bufs[0].iov_base - sp->ring;
  ext.base = offset;
  ext.size = fbuf->bufs[0].iov_len;

  // TODO: Blocking ops in nonblocking context
  fd_write_all(sp->base.fd, &ext, sizeof(ext));

  assert(sp->nr_alloc_nodes <= 3);

  return 1;
}

// Warning: this is a little broken, as there are places where we do blocking reads or writes against the extent socket.
// The hope is this won't make much difference because the extents are unlikely to fill enough space to ever actually block.
void fable_set_nonblocking_shmem_pipe(void* handle) {

  struct shmem_pipe_handle *sp = (struct shmem_pipe_handle*)handle;
  setnb_fd(sp->fd);
  
}

int action_needs_fd(struct shmem_pipe_handle_conn* sp, int type) {

   if(type == FABLE_SELECT_ACCEPT) {
     return 1;
   }
   else if(type == FABLE_SELECT_READ) {
     assert(sp->direction == FABLE_DIRECTION_RECV);
     return (sp->incoming_bytes_consumed - sp->incoming_bytes < sizeof(struct extent));
     // Otherwise at least one extent is waiting to be read, so we're ready now!
   }
   else {
     assert(sp->direction == FABLE_DIRECTION_SEND);
     return !any_shared_space(sp);
     // Otherwise there's at least one free slot. It might be tiny, but write() could do something, so we must report we're writable.
   }

}

void fable_get_select_fds_shmem_pipe(void* handle, int type, int* maxfd, fd_set* rfds, fd_set* wfds, fd_set* efds, struct timeval* timeout) {

   struct shmem_pipe_handle_conn *sp = (struct shmem_pipe_handle_conn*)handle;

   if(action_needs_fd(sp, type)) {
     FD_SET(sp->base.fd, rfds);
     if(sp->base.fd >= *maxfd)
       *maxfd = (sp->base.fd + 1);
   }
   else {
     timeout->tv_sec = 0;
     timeout->tv_usec = 0;
   }

 }

 int fable_ready_shmem_pipe(void* handle, int type, fd_set* rfds, fd_set* wfds, fd_set* efds) {

   struct shmem_pipe_handle_conn *sp = (struct shmem_pipe_handle_conn*)handle;

   if(action_needs_fd(sp, type))
     return FD_ISSET(sp->base.fd, rfds);
   else
     return 1;

 }


