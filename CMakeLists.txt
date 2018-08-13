cmake_minimum_required(VERSION 2.8.3)
project(mongodb_log)

find_package(catkin REQUIRED COMPONENTS roslib)

find_package(Boost REQUIRED COMPONENTS thread filesystem)

set(COMPILE_DEBUG 0)

if(CMAKE_COMPILER_IS_GNUCXX)
  if(COMPILE_DEBUG)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pg")
  else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O2")
  endif()
endif()

catkin_package(
  INCLUDE_DIRS ${catkin_INCLUDE_DIRS}
  LIBRARIES
  CATKIN_DEPENDS roslib 
)


include_directories(
  ${catkin_INCLUDE_DIRS}
  ${Boost_INCLUDE_DIRS})
