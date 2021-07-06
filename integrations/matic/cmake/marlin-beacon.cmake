find_package(MarlinBeacon QUIET)
if(NOT MarlinBeacon_FOUND)
	message("-- MarlinBeacon not found. Using internal MarlinBeacon.")
    include(FetchContent)
    FetchContent_Declare(MarlinBeacon
        GIT_REPOSITORY https://gitlab.com/marlinprotocol/beacon.cpp.git
        GIT_TAG master
    )
    FetchContent_MakeAvailable(MarlinBeacon)

    add_library(Marlin::beacon ALIAS beacon)
else()
	message("-- MarlinBeacon found. Using system MarlinBeacon.")
endif()
