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
find_package(spdlog CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(CURL REQUIRED)
pkg_check_modules(LIBEVENT REQUIRED libevent)


# llama.cpp — built as part of this project via add_subdirectory.
# EXCLUDE_FROM_ALL keeps llama.cpp's own example binaries from being built
# unless explicitly requested. Set LLAMA_DIR on the cmake command line if
# your checkout is somewhere other than ~/llama.cpp:
#   cmake -DLLAMA_DIR=/path/to/llama.cpp ..
set(LLAMA_DIR "${CMAKE_SOURCE_DIR}/third_party/llama.cpp" CACHE PATH "Path to llama.cpp source checkout")
# Disable everything in llama.cpp we don't need — keeps build times down
set(LLAMA_BUILD_TESTS    OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_SERVER   OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS    OFF CACHE BOOL "" FORCE) ##

add_subdirectory(${LLAMA_DIR} ${CMAKE_BINARY_DIR}/llama_build EXCLUDE_FROM_ALL)

include_directories(${PG_INCLUDE_DIR} 
                    ${LIBEVENT_INCLUDE_DIRS} 
		            ${CMAKE_SOURCE_DIR}/include
		            ${CMAKE_SOURCE_DIR}/src/include
		            ${CMAKE_SOURCE_DIR}/src/third_party
		            ${CMAKE_SOURCE_DIR}/src/queue
	    	   )
link_directories(${PG_LIB_DIR} ${LIBEVENT_LIBRARY_DIRS})

# --- shared core library ---
add_library(walkrie_core STATIC
    src/byte_utils.cpp
    src/daemon_utils.cpp
    src/wal_frame.cpp
    src/event_dispatcher.cpp
    src/http_client.cpp
    src/pg_sql_builder.cpp
    src/sink_configuration.cpp
    src/pgsink_configuration.cpp
    src/jsonsink_configuration.cpp
    src/pgoutput_parser.cpp
    src/pgreplication_source.cpp
    src/llama_provider.cpp
    src/openai_provider.cpp
    src/embedding_provider.cpp
    src/pgembedding_sink.cpp
)
target_include_directories(walkrie_core PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src/include
    ${CMAKE_SOURCE_DIR}/src/third_party
    ${CMAKE_SOURCE_DIR}/src/queue
    ${PG_INCLUDE_DIR}
    ${LIBEVENT_INCLUDE_DIRS}
    ${LLAMA_DIR}/include
    ${LLAMA_DIR}/ggml/include
)
target_link_libraries(walkrie_core PUBLIC
    pq
    ${LIBEVENT_LIBRARIES}
    nlohmann_json::nlohmann_json
    spdlog::spdlog_header_only
    CURL::libcurl
    llama
    ggml
)

# --- main binary ---
add_executable(walkrie src/main.cpp)
target_link_libraries(walkrie PRIVATE walkrie_core)

# --- tests ---
add_executable(walkrie_tests
    src/tests/test_main.cpp
    src/tests/test_event_dispatcher.cpp
    src/tests/test_config.cpp
    src/tests/test_config_model.cpp
    src/tests/test_upsert_sql.cpp
    src/tests/test_xframe_log.cpp
    src/tests/test_parser.cpp
)
target_include_directories(walkrie_tests PRIVATE
    src
    src/include
    src/third_party
)
target_link_libraries(walkrie_tests PRIVATE walkrie_core)

# --- bench ---
add_executable(walkrie_bench
    src/bench/walkrie_bench.cpp
    src/bench/resource_sampler.cpp
)
target_include_directories(walkrie_bench PRIVATE
    src
    src/include
    src/third_party
)
target_link_libraries(walkrie_bench PRIVATE walkrie_core)

# --- embedding bench ---
add_executable(embedding_bench src/bench/embedding_bench.cpp)
target_include_directories(embedding_bench PRIVATE
    src
    src/include
    src/third_party
)
target_link_libraries(embedding_bench PRIVATE walkrie_core)
#
add_executable(pg_upsert_bench src/bench/pg_upsert_bench.cpp)
target_include_directories(pg_upsert_bench PRIVATE
    src
    src/include
    src/third_party
)
target_link_libraries(pg_upsert_bench PRIVATE walkrie_core)

# --- demo ---
add_executable(candidate_search demo/candidate_search_tool.cpp)
target_include_directories(candidate_search PRIVATE src src/include src/third_party)
target_link_libraries(candidate_search PRIVATE walkrie_core)

enable_testing()
add_test(NAME unit_tests COMMAND walkrie_tests)

# -- packaging --
# Add install rules (needed for `make install` / packaging):
install(TARGETS walkrie RUNTIME DESTINATION bin)
install(FILES config_sample.toml DESTINATION share/walkrie RENAME config.toml.example)

