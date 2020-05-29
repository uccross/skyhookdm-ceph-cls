cmake_minimum_required(VERSION 3.17)
project(skyhookdm)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules/")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall -g")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror -Wall -g")

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
if (COMPILER_SUPPORTS_CXX11)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
  message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support.")
endif()

find_package(librados)
if(NOT ${LIBRADOS_FOUND})
  message(FATAL_ERROR "Cannot find librados")
endif()

find_package(re2)
if(NOT ${RE2_FOUND})
  message(FATAL_ERROR "Cannot find re2")
endif()

find_package(Boost COMPONENTS date_time program_options REQUIRED)
find_package(Arrow REQUIRED)
#find_package(Parquet REQUIRED)

add_subdirectory(src/cls)
add_subdirectory(src/query)
