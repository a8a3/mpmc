cmake_minimum_required(VERSION 3.20)
project(mpmc CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(mpmc INTERFACE)
target_include_directories(mpmc INTERFACE include)

enable_testing()
add_subdirectory(tests)
