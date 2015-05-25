#include <iostream>
#include <fstream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "message.h"
#include "node.h"

// Experiment Setup.
DEFINE_int32(nracks, 1, "#racks.");
DEFINE_int32(nodes_per_rack, 1, "#nodes per rack.");
DEFINE_int32(fanout, 2, "Fanout at rack-level.");
DEFINE_int32(msg_size, 32, "Message size in bytes.");
DEFINE_int64(in_limit, 1024 * 1024 * 1024, "Inbound BW limit per second at a node.");
DEFINE_int64(out_limit, 1024 * 1024 * 1024, "Outbound BW limit per second at a node.");
DEFINE_int64(in_limit_root, 1024 * 1024 * 1024, "Inbound BW limit per second at a node.");
DEFINE_int64(out_limit_root, 1024 * 1024 * 1024, "Outbound BW limit per second at a node.");

// System Parameters.
DEFINE_int64(duration, 120, "Duration of a simulation in seconds.");
DEFINE_int32(ticks, 1000, "#ticks in a second during simulation.");
DEFINE_int32(nthreads, 1, "Number of worker threads.");

int total_nodes;
std::vector<Node> nodes;

void init() {
  // TODO(haoyan): set the data rate for each node (msgs_per_tick)!

  total_nodes = FLAGS_nracks * FLAGS_nodes_per_rack;

  nodes = std::vector<Node>(total_nodes);

  // Construct a tree.
  // Within a rack, all the nodes send data to the first node in
  // the tree.
  // At rack-level, the first rack is selected as the root, and
  // we build the tree like a "heap" according to increasing
  // rack id.
  // The parennt of the root node is set to -1.
  int lo = 0, hi = 0;
  while (lo < total_nodes) {
    if (lo == 0) {
      nodes[0].p = -1;
      nodes[lo].in = nodes[lo].out = 0;
      nodes[lo].in_limit = FLAGS_in_limit_root / FLAGS_ticks;
      nodes[lo].out_limit = FLAGS_out_limit_root / FLAGS_ticks;
    } else {
      nodes[lo].in = nodes[lo].out = 0;
      nodes[lo].in_limit = FLAGS_in_limit / FLAGS_ticks;
      nodes[lo].out_limit = FLAGS_out_limit / FLAGS_ticks;
    }

    if (hi < total_nodes) {
      for (int i = 0; i < FLAGS_fanout; ++i) {
        hi += FLAGS_nodes_per_rack;
        if (hi >= total_nodes) break;
        nodes[hi].p = lo;
      }
    }

    for (int i = 1; i < FLAGS_nodes_per_rack; ++i) {
      nodes[lo + i].p = lo;
      nodes[lo + i].in = nodes[lo].out = 0;
      nodes[lo + i].in_limit = FLAGS_in_limit / FLAGS_ticks;
      nodes[lo + i].out_limit = FLAGS_out_limit / FLAGS_ticks;
    }
    
    lo += FLAGS_nodes_per_rack;
  }
}

void simulate() {
  int64_t duration_ms = FLAGS_duration * FLAGS_ticks;

  for (int64_t t = 0; t < duration_ms; ++t) {
    // TODO: distribute nodes across a set of threads.
    for (int i = 0; i < total_nodes; ++i) {
      nodes[i].in = nodes[i].out = 0;

      // Step 1: admit incoming messages until nothing left or we hit the BW limit.
      while (!nodes[i].q.empty() && nodes[i].in + FLAGS_msg_size <= nodes[i].in_limit) {
        nodes[i].buf.push_back(nodes[i].q.front());
        nodes[i].q.pop();
        nodes[i].in += FLAGS_msg_size;
      }

      // Step 2: generate its own messages at a certain rate.
      for (int j = 0; j < nodes[i].msgs_per_tick; ++j) {
        Message msg;
        msg.type = 0;
        msg.key = get_next_key();
        nodes[i].buf.push_back(msg);
      }

      // Step 3: process buffered messages if necessary (e.g. GC).
      // TODO(haoyan).

      // Step 4: emit outgoing messages to other nodes until everything is done or we hit the BW limit.
      while (!nodes[i].buf.empty() && nodes[i].out + FLAGS_msg_size <= nodes[i].out_limit) {
        if (nodes[i].p != -1) {
          nodes[nodes[i].p].q.push(nodes[i].buf.front());
        }
        nodes[i].buf.pop_front();
        nodes[i].out += FLAGS_msg_size;
      }
    }
  }
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  init();
  simulate();
}
