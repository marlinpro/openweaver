#include <marlin/core/fabric/Fabric.hpp>


using namespace marlin::core;

struct Terminal {};

struct Fiber {};

int main() {
    Fabric<Terminal, Fiber> f;

    return 0;
}
