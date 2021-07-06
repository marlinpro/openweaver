cmake_minimum_required(VERSION 3.11 FATAL_ERROR)
project(core VERSION 0.0.1 LANGUAGES CXX)

if("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
	add_definitions(-DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO)
else()
	add_definitions(-DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)
endif()

include(GNUInstallDirs)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

add_library(core STATIC
	src/messages/BaseMessage.cpp
	src/BN.cpp
	src/CidrBlock.cpp
	src/Buffer.cpp
	src/SocketAddress.cpp
)
add_library(marlin::core ALIAS core)

target_include_directories(core
	PUBLIC
		$<INSTALL_INTERFACE:include>
		$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
	PRIVATE
		${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_compile_options(core PRIVATE -Werror -Wall -Wextra -pedantic-errors)
target_compile_features(core PUBLIC cxx_std_20)

set_target_properties(core PROPERTIES
	OUTPUT_NAME "marlin-core"
)

# libuv
target_link_libraries(core PUBLIC libuv::uv_a)

# spdlog
target_link_libraries(core PUBLIC spdlog::spdlog_header_only)

# absl::flat_hash_map
target_link_libraries(core PUBLIC absl::flat_hash_map)

install(TARGETS core
	EXPORT marlin-core-export
	LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
	ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

##########################################################
# Export
##########################################################

install(EXPORT marlin-core-export
	FILE
		MarlinCoreTargets.cmake
	NAMESPACE
		marlin::
	DESTINATION
		${CMAKE_INSTALL_LIBDIR}/cmake/marlinCore
)

install(FILES
	${CMAKE_CURRENT_LIST_DIR}/cmake/MarlinCoreConfig.cmake
	DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/marlinCore
)


##########################################################
# Docs
##########################################################

find_package(Doxygen REQUIRED)
add_custom_target(core_docs
	COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile
	WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
	COMMENT "Generating API documentation with Doxygen"
	VERBATIM
)


##########################################################
# Tests
##########################################################

enable_testing()

set(TEST_SOURCES
	test/testBN.cpp
	test/testBuffer.cpp
	test/testEndian.cpp
	test/testSocketAddress.cpp
	test/testFabric.cpp
)

add_custom_target(core_tests)
foreach(TEST_SOURCE ${TEST_SOURCES})
	get_filename_component(TEST_NAME ${TEST_SOURCE} NAME_WE)
	add_executable(${TEST_NAME} ${TEST_SOURCE})
	target_link_libraries(${TEST_NAME} PUBLIC GTest::GTest GTest::Main core)
	target_compile_options(${TEST_NAME} PRIVATE -Werror -Wall -Wextra -pedantic-errors)
	target_compile_features(${TEST_NAME} PRIVATE cxx_std_17)
	add_test(${TEST_NAME} ${TEST_NAME})

	add_dependencies(core_tests ${TEST_NAME})
endforeach(TEST_SOURCE)


##########################################################
# Examples
##########################################################

set(EXAMPLE_SOURCES
	examples/fabric.cpp
)

add_custom_target(core_examples)
foreach(EXAMPLE_SOURCE ${EXAMPLE_SOURCES})
	get_filename_component(EXAMPLE_NAME ${EXAMPLE_SOURCE} NAME_WE)
	add_executable(${EXAMPLE_NAME}_example ${EXAMPLE_SOURCE})
	target_link_libraries(${EXAMPLE_NAME}_example PUBLIC core)
	target_compile_options(${EXAMPLE_NAME}_example PRIVATE -Werror -Wall -Wextra -pedantic-errors)
	target_compile_features(${EXAMPLE_NAME}_example PRIVATE cxx_std_17)

	add_dependencies(core_examples ${EXAMPLE_NAME}_example)
endforeach(EXAMPLE_SOURCE)


##########################################################
# All
##########################################################

add_custom_target(core_all)
add_dependencies(core_all core core_tests core_examples)
