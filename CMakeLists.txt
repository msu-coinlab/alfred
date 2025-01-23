
cmake_minimum_required(VERSION 3.14)
project(alfred)

# =========================
# 1. Set C++ Standard
# =========================
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# =========================
# 2. Compiler Flags
# =========================
# Append the -g flag for debugging without overwriting existing flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

# =========================
# 3. Boost Configuration
# =========================
set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)

# Add Boost compile definitions globally
add_compile_definitions(BOOST_ALL_NO_LIB BOOST_LOG_DYN_LINK=1)

# Find the required Boost components
find_package(Boost 1.74.0 COMPONENTS thread log log_setup REQUIRED)
if(NOT Boost_FOUND)
    message(FATAL_ERROR "Boost not found!")
endif()

# =========================
# 4. Package Configuration
# =========================
find_package(PkgConfig REQUIRED)

# Find and configure dependencies using pkg-config
pkg_check_modules(PROTOBUF REQUIRED IMPORTED_TARGET protobuf)
pkg_check_modules(SNAPPY REQUIRED IMPORTED_TARGET snappy)
pkg_check_modules(CURLPP REQUIRED IMPORTED_TARGET curlpp)
pkg_check_modules(LIBCURL REQUIRED IMPORTED_TARGET libcurl)

# =========================
# 5. Other Dependencies
# =========================
find_package(Parquet CONFIG REQUIRED)
if(NOT Parquet_FOUND)
    message(FATAL_ERROR "Parquet not found!")
endif()

find_package(Arrow CONFIG REQUIRED)
if(NOT Arrow_FOUND)
    message(FATAL_ERROR "Arrow not found!")
endif()

find_package(Threads REQUIRED)

find_package(aws-cpp-sdk-core CONFIG REQUIRED)
if(NOT aws-cpp-sdk-core_FOUND)
    message(FATAL_ERROR "AWS Core SDK not found!")
endif()

find_package(aws-cpp-sdk-s3 CONFIG REQUIRED)
if(NOT aws-cpp-sdk-s3_FOUND)
    message(FATAL_ERROR "AWS S3 SDK not found!")
endif()

# =========================
# 6. Source and Include Directories
# =========================
set(SOURCE_DIR "${CMAKE_SOURCE_DIR}/src")
set(INCLUDE_DIR "${CMAKE_SOURCE_DIR}/include")

# =========================
# 7. Executable Definition
# =========================
add_executable(alfred
    ${SOURCE_DIR}/alfred.cpp 
    ${SOURCE_DIR}/config.cpp 
    ${SOURCE_DIR}/utils.cpp 
    ${SOURCE_DIR}/myawss3.cpp 
    ${SOURCE_DIR}/myamqp.cpp 
    ${SOURCE_DIR}/file_conversion.cpp
)

# Include directories for the executable
target_include_directories(alfred PUBLIC ${INCLUDE_DIR})

# =========================
# 8. Linking Libraries
# =========================
target_link_libraries(alfred PUBLIC 
    Boost::log 
    Boost::log_setup 
    hiredis 
    redis++ 
    fmt 
    arrow_shared 
    parquet 
    aws-cpp-sdk-core 
    aws-cpp-sdk-s3 
    SimpleAmqpClient 
    Threads::Threads 
    PkgConfig::LIBCURL 
    PkgConfig::PROTOBUF 
    PkgConfig::SNAPPY
)

# =========================
# 9. Installation Rules
# =========================
install(TARGETS alfred RUNTIME DESTINATION /usr/local/bin)




