CXX		= g++
CXXFLAGS	= -g -std=c++0x -O2 -lglog -lgflags

all: sim

sim: sim.cc node.h message.h
	$(CXX) $(CXXFLAGS) -o sim sim.cc

clean:
	rm -rf *.o *_test sim *.dSYM/
