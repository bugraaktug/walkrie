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


# llama.cpp — built as part of this project via add_subdirectory.
# EXCLUDE_FROM_ALL keeps llama.cpp's own example binaries from being built
# unless explicitly requested. Set LLAMA_DIR on the cmake command line if
# your checkout is somewhere other than ~/llama.cpp:
#   cmake -DLLAMA_DIR=/path/to/llama.cpp ..
set(LLAMA_DIR "$ENV{HOME}/llama.cpp" CACHE PATH "Path to llama.cpp source checkout")
# Disable everything in llama.cpp we don't need — keeps build times down
set(LLAMA_BUILD_TESTS    OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_SERVER   OFF CACHE BOOL "" FORCE)

add_subdirectory(${LLAMA_DIR} ${CMAKE_BINARY_DIR}/llama_build EXCLUDE_FROM_ALL)

include_directories(${PG_INCLUDE_DIR} 
		    ${LIBEVENT_INCLUDE_DIRS} 
		    ${CMAKE_SOURCE_DIR}/include
		    ${CMAKE_SOURCE_DIR}/src/queue
	    	   )
link_directories(${PG_LIB_DIR} ${LIBEVENT_LIBRARY_DIRS})

add_executable(walkrie
  src/main.cpp
  src/byte_utils.cpp
  src/event_dispatcher.cpp
  src/pgoutput_parser.cpp
  src/pgreplication_source.cpp
  src/pgembedding_sink.cpp
)

target_include_directories(walkrie PRIVATE
  ${LLAMA_DIR}/include
  ${LLAMA_DIR}/ggml/include
)

target_link_libraries(walkrie PRIVATE 
		      pq
		      ${LIBEVENT_LIBRARIES}
		      nlohmann_json::nlohmann_json
		      llama
		      ggml
)
