#ifndef MESSAGE_H_
#define MESSAGE_H_

struct Message {
  // Type of message: 0 -- data; 1 -- tombstone.
  int type;
  // The key for a data message.
  // Only used if this is *not* a tombstone.
  int64_t key;
  // Time when this message should be forwarded.
  // Only used at node buffer.
  int64_t time;
};

#endif // MESSAGE_H_
