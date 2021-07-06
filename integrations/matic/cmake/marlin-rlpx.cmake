find_package(MarlinRlpx QUIET)
if(NOT MarlinRlpx_FOUND)
	message("-- MarlinRlpx not found. Using internal MarlinRlpx.")
    include(FetchContent)
    FetchContent_Declare(MarlinRlpx
        GIT_REPOSITORY https://gitlab.com/marlinprotocol/rlpx.cpp.git
        GIT_TAG master
    )
    FetchContent_MakeAvailable(MarlinRlpx)

    add_library(Marlin::rlpx ALIAS rlpx)
else()
	message("-- MarlinRlpx found. Using system MarlinRlpx.")
endif()
