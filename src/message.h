#ifndef MESSAGE_H_
#define MESSAGE_H_

struct Message {
  // Type of message: 0 -- data; 1 -- tombstone.
  int type;
  // The key for a data message.
  // Only used if this is *not* a tombstone.
  int64_t key;  
};

#endif // MESSAGE_H_
