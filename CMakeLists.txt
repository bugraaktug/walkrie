cmake_minimum_required(VERSION 3.16)
project(walkrie CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

add_compile_options(-Wall -Wextra)

# Locate libpq. On most distros `pg_config` tells us where headers/libs live.
find_program(PG_CONFIG pg_config)
if(PG_CONFIG)
  execute_process(COMMAND ${PG_CONFIG} --includedir
                   OUTPUT_VARIABLE PG_INCLUDE_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
  execute_process(COMMAND ${PG_CONFIG} --libdir
                   OUTPUT_VARIABLE PG_LIB_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
else()
  message(WARNING "pg_config not found; falling back to system default paths (libpq-dev)")
  set(PG_INCLUDE_DIR "/usr/include/postgresql")
  set(PG_LIB_DIR "/usr/lib")
endif()


find_package(nlohmann_json CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBEVENT REQUIRED libevent)

include_directories(${PG_INCLUDE_DIR} ${LIBEVENT_INCLUDE_DIRS} ${CMAKE_SOURCE_DIR}/include)
link_directories(${PG_LIB_DIR} ${LIBEVENT_LIBRARY_DIRS})

add_executable(walkrie
  src/main.cpp
  src/byte_utils.cpp
  src/pgoutput_parser.cpp
  src/pgreplication_source.cpp
)

target_link_libraries(walkrie PRIVATE 
		      pq
		      ${LIBEVENT_LIBRARIES}
		      nlohmann_json::nlohmann_json)
