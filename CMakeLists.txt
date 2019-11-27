cmake_minimum_required(VERSION 2.8.3)
project(mongodb_log)

find_package(catkin
  REQUIRED libmongocxx_ros
  REQUIRED COMPONENTS roscpp roslib)

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
  CATKIN_DEPENDS roscpp roslib
)

include_directories(
  ${catkin_INCLUDE_DIRS}
  ${Boost_INCLUDE_DIRS})

add_executable(mongodb_log_tf src/mongodb_log_tf.cpp)
add_executable(mongodb_log_pcl src/mongodb_log_pcl.cpp)
add_executable(mongodb_log_img src/mongodb_log_img.cpp)
add_executable(mongodb_log_cimg src/mongodb_log_cimg.cpp)
add_executable(benchmark_produce_tf src/benchmark/produce_tf.cpp)

target_link_libraries(mongodb_log_tf
  ${catkin_LIBRARIES}
  ssl
  crypto
  ${Boost_LIBRARIES}
)

target_link_libraries(mongodb_log_pcl
  ${catkin_LIBRARIES}
  ssl
  crypto
  ${Boost_LIBRARIES}
)

target_link_libraries(mongodb_log_img
  ${catkin_LIBRARIES}
  ssl
  crypto
  ${Boost_LIBRARIES}
)

target_link_libraries(mongodb_log_cimg
  ${catkin_LIBRARIES}
  ssl
  crypto
  ${Boost_LIBRARIES}
)

target_link_libraries(benchmark_produce_tf
  ${catkin_LIBRARIES}
  ssl
  crypto
  ${Boost_LIBRARIES}
)
