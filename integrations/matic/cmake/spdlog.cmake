find_package(spdlog NO_CMAKE_PACKAGE_REGISTRY QUIET)
if(NOT spdlog_FOUND)
	message("-- spdlog not found. Using internal spdlog.")

	include(FetchContent)

	FetchContent_Declare(spdlog
		GIT_REPOSITORY https://github.com/gabime/spdlog.git
		GIT_TAG v1.3.1
	)
	FetchContent_MakeAvailable(spdlog)
else()
	message("-- spdlog found. Using system spdlog.")
endif()
