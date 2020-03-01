#ifndef BLOCKCACHE_H_
#define BLOCKCACHE_H_

#include <memory>
#include <unordered_map>

#include "../Block/Block.h"
#include "../../../helpers/Logger/easylogging.h"

class BlockCache {
private:
	std::unordered_map<int, std::shared_ptr<Block>> blockIdBodyMap; 

public:
	bool insert(std::shared_ptr<Block> blockPtr);
	std::shared_ptr<Block> getBlockById(int blockId) const;
	bool hasBlockId(int blockId);
};

#endif /*BLOCKCACHE_H_*/
