
#ifndef FABLE_HELPERS_H
#define FABLE_HELPERS_H

void fable_read_all(void* handle, char* buf, int len);
void fable_write_all(void* handle, const char* buf, int len);
void fable_read_all_multi(void** handles, std::ostream** streams, unsigned int nstreams);

#endif
