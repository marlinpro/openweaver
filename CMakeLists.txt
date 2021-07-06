cmake_minimum_required(VERSION 3.13 FATAL_ERROR)
project(marlin VERSION 0.0.1 LANGUAGES CXX)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
	cmake_policy(SET CMP0069 NEW)
	set(CMAKE_POLICY_DEFAULT_CMP0069 NEW)
	set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

add_subdirectory(vendor EXCLUDE_FROM_ALL)

option(COVERAGE "Code coverage" OFF)
if(COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
	add_compile_options(--coverage)
	add_link_options(--coverage)

	file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/coverage)
	add_custom_target(coverage
		COMMAND gcovr -r ${CMAKE_CURRENT_SOURCE_DIR} --gcov-executable 'llvm-cov gcov' --html-details ${CMAKE_CURRENT_BINARY_DIR}/coverage/index.html -e '.*/?vendor/.*' -e '.*/?build/.*' -e '.*/examples/.*'
	)
endif()

enable_testing()
add_custom_target(docs
	COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile
	WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
	COMMENT "Generating API documentation with Doxygen"
	VERBATIM
)

add_subdirectory(core)
add_subdirectory(simulator)
add_subdirectory(mtest)
add_subdirectory(uvpp)
add_subdirectory(asyncio)
add_subdirectory(beacon)
add_subdirectory(stream)
add_subdirectory(lpf)
add_subdirectory(abci/bsc)
add_subdirectory(abci/cosmos)
add_subdirectory(pubsub)
add_subdirectory(compression)
add_subdirectory(multicastsdk)
add_subdirectory(relay)
add_subdirectory(goldfish)
add_subdirectory(rlpx)
add_subdirectory(integrations/eth)
add_subdirectory(integrations/bsc)
add_subdirectory(integrations/near)
add_subdirectory(integrations/matic)
