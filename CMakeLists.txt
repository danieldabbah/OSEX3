cmake_minimum_required(VERSION 3.22)
project(OSEX3)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
set(CMAKE_CXX_STANDARD 11)

add_executable(OSEX3 main.cpp MapReduceClient.cpp)
