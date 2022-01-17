# define the C compiler to use
CC = g++

# define any compile-time flags
CFLAGS = -Wall -g -O2 -std=c++17

# define any directories containing header files other than /usr/include
#
INCLUDES = -I/usr/include -I/usr/local/include

# define library paths in addition to /usr/lib
#   if I wanted to include libraries not in /usr/lib I'd specify
#   their path using -Lpath, something like:
LFLAGS = -L/usr/lib -L/usr/local/lib

# define any libraries to link into executable:
#   if I want to link in libraries (libx.so or libx.a) I use the -llibname 
#   option, something like (this will link in libmylib.so and libm.so:
LIBS = -lm -lboost_serialization -lboost_system

# define the executable file 
MAIN = kvstore generator

#
# The following part of the makefile is generic; it can be used to 
# build any executable just by changing the definitions above and by
# deleting dependencies appended to the file from 'make depend'
#

.PHONY: depend clean

all: $(MAIN)

$(MAIN): %: %.cc
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $< $(LFLAGS) $(LIBS) 

clean:
	$(RM) *.o *~ $(MAIN) kvstore.data.*

depend: $(SRCS)
	makedepend $(INCLUDES) $^

# DO NOT DELETE THIS LINE -- make depend needs it