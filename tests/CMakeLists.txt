find_package(GTest QUIET)

if(NOT GTest_FOUND)
    include(FetchContent)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.15.2
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
endif()

add_executable(mpmc_tests
    async_mpmc_queue_test.cpp
)

target_link_libraries(mpmc_tests
    PRIVATE
        mpmc
        GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(mpmc_tests)
