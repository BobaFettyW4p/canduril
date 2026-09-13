cmake_minimum_required(VERSION 3.20)
project(canduril LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)

# Locate nanobind's CMake package (installed into the build venv via the
# build-system.requires entry in pyproject.toml).
execute_process(
  COMMAND "${Python_EXECUTABLE}" -c "import nanobind; print(nanobind.cmake_dir())"
  OUTPUT_STRIP_TRAILING_WHITESPACE
  OUTPUT_VARIABLE NB_DIR
)
list(APPEND CMAKE_PREFIX_PATH "${NB_DIR}")
find_package(nanobind CONFIG REQUIRED)

find_package(OpenSSL REQUIRED)

include(FetchContent)
FetchContent_Declare(
  simdjson
  GIT_REPOSITORY https://github.com/simdjson/simdjson.git
  GIT_TAG v3.10.1
  GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(simdjson)

nanobind_add_module(
  _core
  LTO
  src/core/shard.cpp
  src/core/codec.cpp
  src/core/bindings.cpp
)
target_include_directories(_core PRIVATE include)
target_link_libraries(_core PRIVATE simdjson::simdjson OpenSSL::Crypto)

install(TARGETS _core LIBRARY DESTINATION canduril)
