#------------------------------------------------------------------------------
# Copyright (c) 2007-2011, Stanford University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of Stanford University nor the names of its 
#       contributors may be used to endorse or promote products derived from 
#       this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#------------------------------------------------------------------------------ 

# This Makefile requires GNU make.

HOME = ../..

include $(HOME)/Defines.mk

LIBS += -L$(HOME)/$(LIB_DIR) -l$(PHOENIX)

HIST_OBJS = histogram.o
HIST_UNIX_OBJS = histogram_unix.o
HIST_SHM_OBJS = histogram_shm.o

PROGS = histogram histogram_unix histogram_shm

.PHONY: default all clean

default: all

all: $(PROGS)

histogram: $(HIST_OBJS) $(LIB_DEP)
	$(CXX) $(CFLAGS) -o $@ $(HIST_OBJS) $(LIBS)

histogram_unix: $(HIST_UNIX_OBJS) $(LIB_DEP)
	$(CXX) $(CFLAGS) -o $@ $(HIST_UNIX_OBJS) $(LIBS)

histogram_unix.o: histogram.cpp
	$(CXX) $(CFLAGS) -DCOMM_HACKS -DFABLE_TYPE=unixdomain -c $< -o $@ -I$(HOME)/$(INC_DIR)

histogram_shm: $(HIST_SHM_OBJS) $(LIB_DEP)
	$(CXX) $(CFLAGS) -o $@ $(HIST_SHM_OBJS) $(LIBS)

histogram_shm.o: histogram.cpp
	$(CXX) $(CFLAGS) -DCOMM_HACKS -DFABLE_TYPE=shmem_pipe -c $< -o $@ -I$(HOME)/$(INC_DIR)

%.o: %.cpp
	$(CXX) $(CFLAGS) -c $< -o $@ -I$(HOME)/$(INC_DIR)

clean:
	rm -f $(PROGS) $(HIST_OBJS) $(HIST_UNIX_OBJS) $(HIST_SHM_OBJS)
