#ifndef MESSAGE_H_
#define MESSAGE_H_

struct Message {
  // Type of message: 0 -- data; 1 -- tombstone.
  int type;
  // The key for a data message,
  // or starting posision of a tombstone.
  int64_t key;  
  // The end of a tombstone message (exclusive);
  // only used if this is a tombstone.
  int64_t end;
};

#endif // MESSAGE_H_
