#include <math.h>

#include <iostream>
#include <fstream>
#include <map>
#include <sstream>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "message.h"
#include "node.h"

// Experiment Setup.
DEFINE_int32(nracks, 1, "#racks.");
DEFINE_int32(nodes_per_rack, 1, "#nodes per rack.");
DEFINE_int32(fanout, 2, "Fanout at rack-level.");

DEFINE_int64(msg_rate, 4000, "#messages generated by a node per second.");
DEFINE_int32(msg_size, 32, "Message size in bytes.");

DEFINE_int32(gc_policy, 0, "GC policy.");
DEFINE_int32(gc_period, 10, "GC interval.");
DEFINE_int32(gc_levels, 10, "Only perform GC on top k levels of the tree.");
DEFINE_int32(gc_acc_delay, 100, "Accumulated GC delay from leaf to root.");

DEFINE_int64(in_limit, 125000000,
    "Inbound BW limit per second at a node.");
DEFINE_int64(out_limit, 125000000,
    "Outbound BW limit per second at a node.");
DEFINE_int64(in_limit_root, 125000000,
    "Inbound BW limit per second at a node.");
DEFINE_int64(out_limit_root, 125000000,
    "Outbound BW limit per second at a node.");

// System Parameters.
DEFINE_int64(duration, 60, "Duration of a simulation in seconds.");
DEFINE_int32(ticks, 1000, "#ticks in a second during simulation.");
DEFINE_int32(nthreads, 1, "Number of worker threads.");

int total_nodes;
std::vector<Node> nodes;

// Buffer for random keys.
struct KeyBuffer {
  int size, next, fid;
  std::vector<int64_t> data;
} keys;

// Threads to process per-node messages.
std::vector<std::thread> workers;
std::mutex mutex;

inline int get_tree_levels(int fanout, int nodes) {
  return ceil(log((fanout - 1) * nodes + 1) / log(fanout));
}

void init() {
  keys.size = 1024 * 1024 * 1024;
  keys.size /= 32;
  keys.next = keys.size;
  keys.fid = -1;

  total_nodes = FLAGS_nracks * FLAGS_nodes_per_rack;
  LOG(INFO) << "Simulate a datacenter with " << total_nodes << " nodes.";

  nodes = std::vector<Node>(total_nodes);
  for (auto& node : nodes) {
    node.msgs_per_tick = FLAGS_msg_rate / FLAGS_ticks;
  }

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
      nodes[0].level = 0;
    }

    nodes[lo].in = nodes[lo].out = 0;
    nodes[lo].in_limit = FLAGS_in_limit / FLAGS_ticks;
    nodes[lo].out_limit = FLAGS_out_limit / FLAGS_ticks;
    nodes[lo].gc = false;
    nodes[lo].gc_delay = 0;

    if (hi < total_nodes) {
      for (int i = 0; i < FLAGS_fanout; ++i) {
        hi += FLAGS_nodes_per_rack;
        if (hi >= total_nodes) break;
        nodes[hi].p = lo;
        nodes[hi].level = nodes[lo].level + 1;
      }
    }

    for (int i = 1; i < FLAGS_nodes_per_rack; ++i) {
      nodes[lo + i].p = lo;
      nodes[lo + i].in = nodes[lo].out = 0;
      nodes[lo + i].in_limit = FLAGS_in_limit / FLAGS_ticks;
      nodes[lo + i].out_limit = FLAGS_out_limit / FLAGS_ticks;
      nodes[lo + i].gc = false;   // Leaf nodes should not do GC.
    }
    
    lo += FLAGS_nodes_per_rack;
  }

  for (int i = 0; i < total_nodes; i += FLAGS_nodes_per_rack) {
    LOG(INFO) << i << ' ' << nodes[i].p;
  }

  // Set up which nodes should perform Garbage Collections.
  int levels = get_tree_levels(FLAGS_fanout, total_nodes);
  double base_delay;

  switch (FLAGS_gc_policy) {
    case 0: // No GC.
      break;
    case 1: // All rack hubs; uniform delay.
      base_delay = FLAGS_gc_acc_delay / double(levels);
      for (int i = 0; i < total_nodes; i += FLAGS_nodes_per_rack) {
        nodes[i].gc = true;
        nodes[i].gc_delay = base_delay;
      }
      break;
    case 2: // All rack hubs; linearly decrease delays downwards.
      base_delay = FLAGS_gc_acc_delay / double((levels + 1) * levels / 2);
      for (int i = 0; i < total_nodes; i += FLAGS_nodes_per_rack) {
        nodes[i].gc = true;
        nodes[i].gc_delay = base_delay * (levels - nodes[i].level);
      }
      break;
    case 3: // All rack hubs; linearly increase delays downwards.
      base_delay = FLAGS_gc_acc_delay / double((levels + 1) * levels / 2);
      for (int i = 0; i < total_nodes; i += FLAGS_nodes_per_rack) {
        nodes[i].gc = true;
        nodes[i].gc_delay = base_delay * (nodes[i].level + 1);
      }
      break;
    case 4: // Only top k levels; uniform delay.
      if (levels > FLAGS_gc_levels) {
        levels = FLAGS_gc_levels;
      }
      base_delay = FLAGS_gc_acc_delay / double(levels);
      for (int i = 0; i <= total_nodes; i += FLAGS_nodes_per_rack) {
        if (nodes[i].level >= levels) break;
        nodes[i].gc = true;
        nodes[i].gc_delay = base_delay;
      }
      break;
    case 5: // Only top k levels; linearly decrease delays downwards.
      if (levels > FLAGS_gc_levels) {
        levels = FLAGS_gc_levels;
      }
      base_delay = FLAGS_gc_acc_delay / double((levels + 1) * levels / 2);
      for (int i = 0; i <= total_nodes; i += FLAGS_nodes_per_rack) {
        if (nodes[i].level >= levels) break;
        nodes[i].gc = true;
        nodes[i].gc_delay = base_delay * (levels - nodes[i].level);
      }
      break;
    case 6: // Only top k levels; linearly increase delays downwards.
      if (levels > FLAGS_gc_levels) {
        levels = FLAGS_gc_levels;
      }
      base_delay = FLAGS_gc_acc_delay / double((levels + 1) * levels / 2);
      for (int i = 0; i <= total_nodes; i += FLAGS_nodes_per_rack) {
        if (nodes[i].level >= levels) break;
        nodes[i].gc = true;
        nodes[i].gc_delay = base_delay * (nodes[i].level + 1);
      }
      break;
    default:
      break;
  }
}

int64_t get_next_key() {
  int64_t result;

  mutex.lock();
  if (keys.next == keys.size) {
    ++keys.fid;
    std::stringstream filename;
    filename << "data-" << keys.fid;

    std::fstream keyfile(filename.str());

    keys.data.clear();
    for (int i = 0; i < keys.size; ++i) {
      int64_t x;
      keyfile >> x;
      CHECK(x >= 0) << filename.str() << ' ' << i << ' ' << x;
      keys.data.push_back(x);
    }

    keyfile.close();

    keys.next = 1;
    CHECK(keys.data[0] >= 0) << 0 << ' ' << keys.data[0];
    result = keys.data[0];
  } else {
    CHECK(keys.data[0] >= 0) << keys.next << ' ' << keys.data[keys.next];
    result = keys.data[keys.next++];
  }

  mutex.unlock();
  return result;
}

void write_log(int t) {
  LOG(INFO) << (t / FLAGS_ticks) << " seconds";

  int64_t total_self = 0;
  int64_t total_saved = 0;
  for (int i = 0; i < total_nodes; ++i) {
    total_self += nodes[i].self_per_sec;
    total_saved += nodes[i].saved_per_sec;
  }

  LOG(INFO) << "Total data generated: " << total_self;
  LOG(INFO) << "Total space saved: " << total_saved;

  for (int i = 0; i < total_nodes; ++i) {
    if (i % FLAGS_nodes_per_rack == 0) {
      LOG(INFO) << "Node " << i
        // Total input in MB: subtree + self-generated
        << ' ' << double(nodes[i].in_per_sec + nodes[i].self_per_sec) / 1024 / 1024
        // Total output in MB
        << ' ' << double(nodes[i].out_per_sec) / 1024 / 1024
        // Total effective output in MB
        << ' ' << double(nodes[i].eff_out_per_sec) / 1024 / 1024
        // %inbound BW usage
        << ' ' << double(nodes[i].in_per_sec) / (nodes[i].in_limit * FLAGS_ticks) * 100
        // %outbound BW usage
        << ' ' << double(nodes[i].out_per_sec) / (nodes[i].out_limit * FLAGS_ticks) * 100;
    }
    nodes[i].in_per_sec = nodes[i].out_per_sec = nodes[i].eff_out_per_sec = 0;
    nodes[i].self_per_sec = 0;
    nodes[i].saved_per_sec = 0;
  }
}

void process_messages_by_node(int lo, int hi, int t) {
  for (int i = lo; i < hi; ++i) {
//    LOG(INFO) << "Node " << i;
    nodes[i].in = nodes[i].out = 0;

    // Step 1: admit incoming messages until nothing left or we hit the BW limit.
    while (!nodes[i].q.empty() && nodes[i].in + FLAGS_msg_size <= nodes[i].in_limit) {
      Message msg = nodes[i].q.front();
      if (nodes[i].gc) {
        msg.time = t + nodes[i].gc_delay;
      } else {
        msg.time = t;
      }

      nodes[i].buf.push_back(msg);
      nodes[i].q.pop();
      nodes[i].in += FLAGS_msg_size;
      ++nodes[i].total_in_msgs;
    }
    nodes[i].in_per_sec += nodes[i].in;

//    LOG(INFO) << "Step 1 done.";

    // Step 2: generate its own messages at a certain rate.
    for (int j = 0; j < nodes[i].msgs_per_tick; ++j) {
      Message msg;
      msg.type = 0;
      msg.key = get_next_key();
      msg.eff_size = 1;
      if (nodes[i].gc) {
        msg.time = t + nodes[i].gc_delay;
      } else {
        msg.time = t;
      }

      nodes[i].buf.push_back(msg);
    }
    nodes[i].self_per_sec += nodes[i].msgs_per_tick * FLAGS_msg_size;

//    LOG(INFO) << "Step 2 done.";

    // Step 3: process buffered messages if necessary (e.g. GC).
    if (nodes[i].gc && t % FLAGS_gc_period == 0 && !nodes[i].buf.empty()) {
      int pos = int(nodes[i].buf.size());
/*
      LOG(INFO) << "Keys:";
      for (int j = 0; j < pos + 1; ++j) {
        if (nodes[i].buf[j].type != 0) {
          LOG(INFO) << "TS";
        } else {
          LOG(INFO) << nodes[i].buf[j].key;
        }
      }
*/


      // If nodes get acks by receiving other messages (e.g. invalidations) or
      // retry after timeout, no specific "ordering" is required here, so we
      // don't need to maintain tombstones.
      std::map<int, int> table;
      int saved = 0;

      for (int j = 0; j < pos; ++j) {
        if (nodes[i].buf[j].type == 0) {
          if (table.count(nodes[i].buf[j].key) == 1) {
            nodes[i].buf[j].type = 1;
            int idx = table[nodes[i].buf[j].key];
            nodes[i].buf[idx].eff_size += nodes[i].buf[j].eff_size;
//            LOG(INFO) << "key " << nodes[i].buf[j].key << "; idx " << idx;
//            LOG(INFO) << "eff_size " << nodes[i].buf[idx].eff_size;
            ++saved;
          } else {
            table[nodes[i].buf[j].key] = j;
          }
        }
      }
      nodes[i].saved_per_sec += saved * FLAGS_msg_size;
/*
      pos = int(nodes[i].buf.size());
      int flag = 0;
      int idx = -1;
      int saved = 0;
      for (int j = 0; j < pos; ++j) {
        if (nodes[i].buf[j].type == 1) {
          if (flag == 0) {
            flag = 1;
            idx = j;
          } else {
            nodes[i].buf[j].type = 2;
            nodes[i].buf[idx].eff_size += nodes[i].buf[j].eff_size;
            ++saved;//nodes[i].msgs_saved;
          }
        } else if (nodes[i].buf[j].type == 0) {
          flag = 0;
        }
      }
      nodes[i].msgs_saved += saved;
*/
      if (t % (FLAGS_ticks / 2) == 0) {
        mutex.lock();
        LOG(INFO) << "GC at node " << i << ": "
            << saved << "/" << pos << ".";
        mutex.unlock();
      }
    }

//    LOG(INFO) << "Step 3 done.";
  }
}

void simulate() {
  int64_t duration_ms = FLAGS_duration * FLAGS_ticks;
  LOG(INFO) << "Simulate for " << duration_ms << " ticks on "
      << FLAGS_nthreads << " threads...";

  // Prepare threads.
  if (FLAGS_nthreads > 1) {
    workers = std::vector<std::thread>(FLAGS_nthreads);
  }


  for (int64_t t = 0; t < duration_ms; ++t) {
    // Distribute nodes across a set of threads if allowed.
    if (FLAGS_nthreads > 1) {
      int nodes_per_thread = total_nodes / FLAGS_nthreads;
      for (int i = 0; i < FLAGS_nthreads; ++i) {
        int begin = nodes_per_thread * i, end = begin + nodes_per_thread;
        if (end > total_nodes) end = total_nodes;

//        LOG(INFO) << "Thread " << i << ": nodes [" << begin << ", " << end << ")";

        workers[i] = std::thread(process_messages_by_node, begin, end, t);
      }

      for (int i = 0; i < FLAGS_nthreads; ++i) {
        workers[i].join();
      }
    } else {
      process_messages_by_node(0, total_nodes, t);
    }

    for (int i = 0; i < total_nodes; ++i) {
      // Step 4: emit outgoing messages to other nodes until everything is done or we hit the BW limit.
      while (!nodes[i].buf.empty()
          && nodes[i].buf.front().time <= t
          && nodes[i].out + FLAGS_msg_size <= nodes[i].out_limit) {
        if (nodes[i].buf.front().type == 0) {
          if (nodes[i].p != -1) {
            nodes[nodes[i].p].q.push(nodes[i].buf.front());
          }
          nodes[i].out += FLAGS_msg_size;
          ++nodes[i].total_out_msgs;

          CHECK(nodes[i].buf.front().eff_size > 0);
          nodes[i].eff_out_per_sec += nodes[i].buf.front().eff_size * FLAGS_msg_size;
        }
        nodes[i].buf.pop_front();
      }
      nodes[i].out_per_sec += nodes[i].out;

//      LOG(INFO) << "Step 4 done.";
    }

    // Gather log information.
    if (t != 0 && t % FLAGS_ticks == 0) {
      write_log(t);
    }
  }

  write_log(duration_ms);
}

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  init();
  simulate();
}
