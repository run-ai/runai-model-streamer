# /opt/toolchains/aarch64-none-linux-gnu.cmake
SET(CMAKE_SYSTEM_NAME Linux)
SET(CMAKE_SYSTEM_PROCESSOR aarch64)

# Compiler and linker paths
SET(CMAKE_C_COMPILER /opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-gcc)
SET(CMAKE_CXX_COMPILER /opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-g++)
SET(CMAKE_LINKER /opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-ld)
SET(CMAKE_AR /opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-ar)
SET(CMAKE_NM /opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-nm)
SET(CMAKE_RANLIB /opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-ranlib)

# Specify sysroot if needed
SET(CMAKE_FIND_ROOT_PATH /opt/aarch64-sysroot)
SET(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
SET(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
SET(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)