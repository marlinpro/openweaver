#ifndef NEWBLOCKIDMESSAGE_H_
#define NEWBLOCKIDMESSAGE_H_

#include "./Message.h"

class NewBlockIdMessage : public Message {
private:
	int blockId;

public:
	NewBlockIdMessage(int blockId);
	int getBlockId();
	std::string getType();
};

#endif /*NEWBLOCKIDMESSAGE_H_*/