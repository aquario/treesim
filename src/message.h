#ifndef MESSAGE_H_
#define MESSAGE_H_

struct Message {
  // Type of message: 0 -- data; 1 -- tombstone.
  int type;
  // The key for a data message.
  // Only used if this is *not* a tombstone.
  int64_t key;
  // Effective size of a message.  This reflects how many other messages
  // it has "absorbed" due to garbage collection.
  int64_t eff_size;
  // Time when this message should be forwarded.
  // Only used at node buffer.
  int64_t time;
  // ID of the tree this message goes through.
  int tree;

  bool operator < (const Message &right) const {
    return time < right.time;
  }
};

#endif // MESSAGE_H_
