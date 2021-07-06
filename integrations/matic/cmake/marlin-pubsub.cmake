find_package(MarlinPubsub QUIET)
if(NOT MarlinPubsub_FOUND)
	message("-- MarlinPubsub not found. Using internal MarlinPubsub.")
    include(FetchContent)
    FetchContent_Declare(MarlinPubsub
        GIT_REPOSITORY https://gitlab.com/marlinprotocol/pubsub.cpp.git
        GIT_TAG master
    )
    FetchContent_MakeAvailable(MarlinPubsub)

    add_library(Marlin::pubsub ALIAS pubsub)
else()
	message("-- MarlinPubsub found. Using system MarlinPubsub.")
endif()
