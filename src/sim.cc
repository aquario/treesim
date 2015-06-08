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

DEFINE_bool(multitree, false, "Use multiple trees?");

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

// Node count and structures.
int total_nodes;
std::vector<Node> nodes;

// Keep the count of Trees and level-order node layout of each tree.
int ntrees;
std::vector< std::vector<int> > trees;

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

int get_internal_node_count(int fanout, int nodes) {
  int sum = 0;
  int x = 1;
  while (sum + x < nodes) {
    sum += x;
    x *= fanout;
  }

  return nodes - ((nodes - sum) + x / fanout - ceil((nodes - sum) / double(fanout)));
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

  // With multiple trees, the number of tree is max(2, fanout - 1).
  if (FLAGS_multitree) {
    if (FLAGS_fanout > 2) {
      ntrees = FLAGS_fanout - 1;
    } else {
      ntrees = 2;
    }
  } else {
    ntrees = 1;
  }

  trees = std::vector< std::vector<int> >(ntrees);
  for (int i = 0; i < ntrees; ++i) {
    trees[i] = std::vector<int>(FLAGS_nracks);
  }

  for (int i = 0; i < total_nodes; i += FLAGS_nodes_per_rack) {
    nodes[i].in = nodes[i].out = 0;
    nodes[i].in_limit = FLAGS_in_limit / FLAGS_ticks;
    nodes[i].out_limit = FLAGS_out_limit / FLAGS_ticks;

    nodes[i].p = std::vector<int>(ntrees);
    nodes[i].level = std::vector<int>(ntrees);
    nodes[i].buf = std::vector< std::deque<Message> >(ntrees);
    nodes[i].gc = std::vector<bool>(ntrees, false);
    nodes[i].gc_delay = std::vector<int>(ntrees, 0);
    for (int j = 1; j < FLAGS_nodes_per_rack; ++j) {
      nodes[i + j].in = nodes[i + j].out = 0;
      nodes[i + j].in_limit = FLAGS_in_limit / FLAGS_ticks;
      nodes[i + j].out_limit = FLAGS_out_limit / FLAGS_ticks;

      // Within a rack, all the nodes send data to the first node in
      // the tree.
      nodes[i + j].p = std::vector<int>(ntrees, i);
      nodes[i + j].level = std::vector<int>(ntrees, -1);
      nodes[i + j].buf = std::vector< std::deque<Message> >(ntrees);
      nodes[i + j].gc = std::vector<bool>(ntrees, false);
      nodes[i + j].gc_delay = std::vector<int>(ntrees, 0);
    }
  }

  // Construct trees.
  // First, get the number of internal nodes in each tree.
  int ninternals = get_internal_node_count(FLAGS_fanout, FLAGS_nracks);
  LOG(INFO) << "#internal nodes = " << ninternals;

  for (int i = 0; i < ntrees; ++i) {
    // For each tree ...
    // Get level-order layout of this tree.
    for (int j = 0; j < FLAGS_nracks; ++j) {
      trees[i][j] = j * FLAGS_nodes_per_rack;
    }
    if (i > 0) {
      for (int j = 0; j < ninternals; ++j) {
        std::swap(trees[i][j], trees[i][j + i * ninternals]);
      }
    }

    // Create links.
    // The parennt of the root node is set to -1.
    nodes[trees[i][0]].p[i] = -1;
    nodes[trees[i][0]].level[i] = 0;

    int lo = 0, hi = 1;
    int cnt = 0;
    while (hi < FLAGS_nracks) {
      if (cnt == FLAGS_fanout) {
        cnt = 0;
        lo++;
      } else {
        cnt++;
        nodes[trees[i][hi]].p[i] = trees[i][lo];
        nodes[trees[i][hi]].level[i] = nodes[trees[i][lo]].level[i] + 1;
        hi++;
      }
    }

  }

  for (int i = 0; i < total_nodes; i += FLAGS_nodes_per_rack) {
    std::stringstream st;
    st << i;
    for (int j = 0; j < ntrees; ++j) {
      st << ' ' << nodes[i].p[j];
    }
    LOG(INFO) << st.str();
  }

  // Set up which nodes should perform Garbage Collections.
  int levels = get_tree_levels(FLAGS_fanout, FLAGS_nracks);
  int diff = 0;
  double base_delay;

  switch (FLAGS_gc_policy) {
    case 0: // No GC.
      break;
    case 1: // All rack hubs; uniform delay.
      base_delay = FLAGS_gc_acc_delay / double(levels);
      for (int i = 0; i < ntrees; ++i) {
        for (int j = 0; j < FLAGS_nracks; j++) {
          nodes[trees[i][j]].gc[i] = true;
          nodes[trees[i][j]].gc_delay[i] = base_delay;
        }
      }
      break;
    case 2: // All rack hubs; linearly decrease delays downwards.
      base_delay = FLAGS_gc_acc_delay / double((levels + 1) * levels / 2);
      for (int i = 0; i < ntrees; ++i) {
        for (int j = 0; j < FLAGS_nracks; j++) {
          nodes[trees[i][j]].gc[i] = true;
          nodes[trees[i][j]].gc_delay[i] = base_delay * (levels - nodes[trees[i][j]].level[i]);
        }
      }
      break;
    case 3: // All rack hubs; linearly increase delays downwards.
      base_delay = FLAGS_gc_acc_delay / double((levels + 1) * levels / 2);
      for (int i = 0; i < ntrees; ++i) {
        for (int j = 0; j < FLAGS_nracks; j++) {
          nodes[trees[i][j]].gc[i] = true;
          nodes[trees[i][j]].gc_delay[i] = base_delay * (nodes[trees[i][j]].level[i] + 1);
        }
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

    LOG(INFO) << "Reading data file " << filename.str() << "...";
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
      Message msg = nodes[i].q.top();
      int tid = msg.tree;
      if (nodes[i].gc[tid]) {
        msg.time = t + nodes[i].gc_delay[tid];
      } else {
        msg.time = t;
      }

      nodes[i].buf[tid].push_back(msg);
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
      msg.tree = (j + t) % ntrees; // Round-robin across trees.
      if (nodes[i].gc[msg.tree]) {
        msg.time = t + nodes[i].gc_delay[msg.tree];
      } else {
        msg.time = t;
      }

      nodes[i].buf[msg.tree].push_back(msg);
    }
    nodes[i].self_per_sec += nodes[i].msgs_per_tick * FLAGS_msg_size;

//    LOG(INFO) << "Step 2 done.";

    // Step 3: process buffered messages if necessary (e.g. GC).
    for (int tree = 0; tree < ntrees; ++tree) {
      if (nodes[i].gc[tree] && t % FLAGS_gc_period == 0 && !nodes[i].buf[tree].empty()) {
        int pos = int(nodes[i].buf[tree].size());

        // If nodes get acks by receiving other messages (e.g. invalidations) or
        // retry after timeout, no specific "ordering" is required here, so we
        // don't need to maintain tombstones.
        std::map<int, int> table;
        int saved = 0;

        for (int j = 0; j < pos; ++j) {
          if (nodes[i].buf[tree][j].type == 0) {
            if (table.count(nodes[i].buf[tree][j].key) == 1) {
              nodes[i].buf[tree][j].type = 1;
              int idx = table[nodes[i].buf[tree][j].key];
              nodes[i].buf[tree][idx].eff_size += nodes[i].buf[tree][j].eff_size;
              //            LOG(INFO) << "key " << nodes[i].buf[j].key << "; idx " << idx;
              //            LOG(INFO) << "eff_size " << nodes[i].buf[idx].eff_size;
              ++saved;
            } else {
              table[nodes[i].buf[tree][j].key] = j;
            }
          }
        }
        nodes[i].saved_per_sec += saved * FLAGS_msg_size;

        if (t % (FLAGS_ticks / 2) == 0) {
          mutex.lock();
          LOG(INFO) << "GC at node " << i << ": "
            << saved << "/" << pos << ".";
          mutex.unlock();
        }
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
      bool flag = true;
      while (flag) {
        flag = false;
        for (int tree = 0; tree < ntrees; ++tree) {
          while (!nodes[i].buf[tree].empty()
              && nodes[i].buf[tree].front().type != 0) {
            nodes[i].buf[tree].pop_front();
          }

          if (!nodes[i].buf[tree].empty()
              && nodes[i].buf[tree].front().time <= t
              && nodes[i].out + FLAGS_msg_size <= nodes[i].out_limit) {
            if (nodes[i].p[tree] != -1) {
              nodes[nodes[i].p[tree]].q.push(nodes[i].buf[tree].front());
            }
            nodes[i].out += FLAGS_msg_size;
            nodes[i].out_per_sec += FLAGS_msg_size;
            ++nodes[i].total_out_msgs;

            CHECK(nodes[i].buf[tree].front().eff_size > 0);
            nodes[i].eff_out_per_sec += nodes[i].buf[tree].front().eff_size * FLAGS_msg_size;
            nodes[i].buf[tree].pop_front();

            flag = true;
          }

          //      LOG(INFO) << "Step 4 done.";
        }
      }
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
