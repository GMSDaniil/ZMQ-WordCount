Distributed Word Count using ZeroMQ
=====================================

Overview
--------
This project implements a distributed word count system using a MapReduce-like approach with ZeroMQ. It consists of two main components:

1. zmq_distributor.c
   - Reads an input text file.
   - Splits the file into chunks (ensuring words are not broken).
   - Sends "map" messages with text chunks to worker processes.
   - Collects map results and aggregates word counts.
   - Sends "reduce" messages to combine counts.
   - Sorts and outputs the final results in CSV format.
   - Sends a "rip" command to all workers to signal termination.

2. zmq_worker.c
   - Runs as a worker process that listens on one or more ports.
   - Creates a REP socket on each specified port.
   - Handles "map...", "red...", and "rip" messages.
   - Processes map and reduce operations and returns the results.
   - Exits when a "rip" message is received.

Dependencies
------------
- ZeroMQ library (libzmq)
- POSIX Threads (pthread)
- C compiler (e.g., gcc)

Compilation
-----------
Compile the distributor and worker programs using gcc. For example:

    cmake -B build -DCMAKE_BUILD_TYPE=Debug
    make -C build

Usage
-----
1. Start one or more worker processes, specifying the ports on which they should listen. For example, to start a workers on ports 5555 and 5556(1 port per worker):
   ```
    ./build/zmq_worker 5555
    ./build/zmq_worker 5556
2. Run the distributor, providing the input file and the workers` ports:
   ```
    ./build/zmq_distributor input.txt 5555 5556
The distributor will process the input file, distribute the workload among the workers, and finally output the word counts in CSV format.

Notes
-----
- Ensure that ZeroMQ is installed on your system before compiling.
- The system uses basic string parsing for word counts and may not handle all edge cases in natural language processing.
- The final output is in CSV format with columns "word" and "frequency".
