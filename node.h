#ifndef NODE_H_
#define NODE_H_

#include <mutex>
#include <queue>

#include "message.h"

struct Node {
  int p;                  // Parent node id in the tree.
  std::queue<Message> q;  // Incoming event queue for this node.

  int64_t in, out;        // In/outbound traffic during current time slice.
  int64_t in_limit, out_limit;    // BW limit for each time slice.

  int64_t total_msgs;     // Bookkeeping the total #msgs processed.
};


#endif // NODE_H_
