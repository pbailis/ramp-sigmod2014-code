
This repo contains the prototype database evaluated in our SIGMOD 2014 paper titled [Scalable Atomic Visibility with RAMP Transactions](http://www.bailis.org/papers/ramp-sigmod2014.pdf) (I posted an introductory blog post [here](http://www.bailis.org/blog/scalable-atomic-visibility-with-ramp-transactions/)).  The codename for this prototype was "kaiju" (which, sadly, [Pacific Rim](http://en.wikipedia.org/wiki/Pacific_Rim_(film)) somewhat robbed of its hip awesomeness](http://en.wikipedia.org/wiki/Kaiju) as a codename).

### To get a flavor of the RAMP Algorithms

I've written up a very simple Python implementation of RAMP-Fast, RAMP-Small, and RAMP-Hybrid in the `py-impl` directory. `demo.py` provides a multi-threaded test harness to play with. In the Python demo, there's no garbage collection or RPC---for that, read on for details on the real code used in the paper.

### Code release notes

The remaining code in this repo is the exact code used for the experiments in the SIGMOD 2014 paper, with additional comments and minor variable renaming.  Where relevant, I've included annotations about particular implementation details but haven't done any refactoring.  I think that, with a little spelunking, the code is understandable (especially if you've read the paper), but a primary goal in releasing this code is scientific---to let others see what I did and how I implemented things---rather than as a production-ready artifact.

The codebase is uglier than I'd like, in part because it contains eight different implementations of distributed transactions as evaluated in the paper.  For example, one of the algorithms broke our RPC model and required a special set of RPC handlers (specifically, my original RPC layer assumed RPC handlers wouldn't trigger additional RPCs, but the aforementioned algorithm required RPC chaining, which broke the handler interface). Keep in mind that this is research code, so the quality varies throughout the repo.

I'd consider stripping this code base down to *just* RAMP-Fast, RAMP-Small, or RAMP-Hybrid if there's interest, but, as it is today, the main code in `src` is still less than 3,600 lines of Java.

### What's implemented?

The code implements a partitioned (sharded) distributed KVS as described in the paper. Clients connect to any server in the KVS, which executes multi-put and multi-get operations on their behalf (see Section 4.5 for a discussion of read-write transactions).

As described in the paper (Section 5.1), the prototype contains implementations of the RAMP-Fast, RAMP-Hybrid, and RAMP-Small algorithms as well as the Eiger 2PC-CI algorithm, three distributed locking algorithms, and a baseline without any concurrency control.

The codebase also contains an implementation of the CTP termination protocol for the RAMP-Fast algorithm and hooks for reproducing the experiments in Section 5.3.

All data is currently stored in memory, and, aside from the CTP protocol implementation, the system is not fault tolerant.

### Getting a handle on the code

The meat of the RAMP algorithms are found in `ReadAtomicKaijuServiceHandler` and the various `ReadAtomic{Bloom, ListBased, Stamp}ServiceHandler` classes.
A lot of heavy lifting also occurs in `MemoryStorageEngine`, which manages prepared and committed versions.

## If you want to run this code:

### Basic instructions:

Build the jar:
`mvn package`

Install the jar into your local maven repository and build the command line client:
`cd contrib/YCSB; bash ./install-kaiju-jar.sh; cd ../..`
`cd tools; mvn package`

Run (a simple test) locally:
`bin/run-local-cluster; sleep 5; bin/run-put-test.sh`

To run full benchmarks, see below.

### Interacting with the database

The client interface is located in `edu.berkeley.kaiju.KaijuClient`. I haven't mavenized this into separate client and server jars,
so, for now, you should just link against the entire jar (see the YCSB pom for an example).

Clients can connect to any server in the cluster, but, for load balancing, you'll likely want to spread out requests. Clients aren't currently thread-safe, but it shouldn't take much work to fix this if desired.

You can also take a look at our YCSB maven integration and client code in `contrib/ycsb`.

### Running Experiments

I've included our harness for running experiments in the `experiments` directory. The scripts currently assume you're running
on `cr1.8xlarge` instances, and you'll need to modify the `AMIs` dictionary in `setup_hosts.py` to point to an AMI
with a user named `ubuntu` and this repo checked out in `/home/ubuntu/kaiju`. While these scripts aren't pristine,
they should be pretty easy to adapt to your needs. `experiment/example.txt` explains how to set up a cluster and run a test, while `experiments/run-all.sh` should run all of the experiments in the paper
except for the scale-out test.

Feel free to reach out if you're having difficulties. It shouldn't be much work to set up the server environment on a stock EC2 Ubuntu AMI.

The experiments have several dependencies, including `parallel-ssh` and `boto` (properly configured for EC2).

### What would you change?

As I mentioned, implementing a bunch of algorithms in one system makes for messy code.  You can get much cleaner code by picking one (say, RAMP-Fast) and focusing on it.

Second---and this is probably the biggest thing---I'd revamp the RPC layer. Gory detail:

We use a CachedThreadPool to execute client requests, which is okay but can lead to degradation (in the form of CPU underutilization and inefficiency due to context switching) under heavy load.  We actually iterated on this a few times, moving from Thrift to a Protobuf-based solution to our current Kryo-based solution.  Getting off Thrift was huge (e.g., we saw huge wins compared to our [HAT](http://www.bailis.org/papers/hat-vldb2014.pdf) [prototype](https://github.com/pbailis/hat-vldb2014-code)), primarily because the original Thrift doesn't allow multiple outstanding requests on a connection (!).  I don't know what Thrift2 has to offer, but I'm not inclined to go back.

In any event, the implementation isn't great about thread usage and doesn't do anything smart about connection pooling. I think this is fine for the results in the paper, but the next generation of database prototypes we've been building more aggressively employ NIO, batching, and *most importantly*, fixed thread pools with Futures to cap utilization and thrashing. The awesome part is that, with appropriately-sized pools, we can fully utilize the 32 `cr1.8xlarge` CPU contexts. The not-so-awesome part is that stack-ripping makes for uglier code (e.g., chaining everything using nested invocations of `f.onComplete { ... }`). With these improvements, it's possible to get upwards of a million requests/sec on a single box, even on the JVM.

I continue to be surprised how important RPC is to high-performance distributed databases---take a look at the [MICA paper](http://www.cs.cmu.edu/~hl/papers/mica-nsdi2014.pdf) to see what you can do with an amazing, non-batched RPC layer (and kernel bypass): 76+ Million requests/sec on a single node! I've had difficulty getting any widely used packages to perform at high utilization on many-core machines (e.g., Thrift, also playing with Netty-based solutions). Maybe this is because most production deployments of RPC-based services don't actually need/operate at such high utilization. However, I think high-performance RPC is an area that's ripe for production-quality open source development.

In any event, these changes would likely increase throughput but not the overall trends in the paper. If you understood Table 1 in the paper, nothing in Section 5 should be surprising.

### Questions/Comments

Let me know if you have any questions. Feedback is always appreciated, and if you're interested in using these algorithms in your database, please [drop me a line](http://www.bailis.org/contact.html).
