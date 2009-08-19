#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_INIT([hornetq-native], [2.0],[clebert.suconic@jboss.org])
AC_CONFIG_AUX_DIR([build-aux])

AM_INIT_AUTOMAKE([dist-bzip2])

# Minimum Autoconf version required.
AC_PREREQ(2.59)

AC_JNI_INCLUDE_DIR
AC_CONFIG_HEADERS([config.h:config.in])
AC_CONFIG_SRCDIR([src/JNI_AsynchronousFileImpl.cpp])

# Check for libaio and libaio-devel
libaio_fail=0
AC_CHECK_LIB([aio], [io_setup], ,[libaio_fail=1])
AC_CHECK_HEADER([libaio.h], ,[libaio_fail=1])
test $libaio_fail == 1 && \
  AC_MSG_ERROR([libaio-devel package missing. Please ensure both libaio and libaio-devel are installed. (hint: yum install libaio-devel should do it...)])
	

gl_COMPILER_FLAGS(-Werror)
#gl_COMPILER_FLAGS(-g) -- enable this for debug
# Compile it with full optimizations
#gl_COMPILER_FLAGS(-O3) -- enable this for full optimizations
gl_COMPILER_FLAGS(-pedantic)
gl_COMPILER_FLAGS(-Wall)
gl_COMPILER_FLAGS(-Wextra)
gl_COMPILER_FLAGS(-Wno-shadow)
gl_COMPILER_FLAGS(-Wpointer-arith)
gl_COMPILER_FLAGS(-Wcast-qual)
gl_COMPILER_FLAGS(-Wcast-align)
gl_COMPILER_FLAGS(-Wno-long-long)
gl_COMPILER_FLAGS(-Wvolatile-register-var)
gl_COMPILER_FLAGS(-Winvalid-pch)
gl_COMPILER_FLAGS(-Wno-system-headers)

CPPFLAGS="$CPPFLAGS $COMPILER_FLAGS"

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_PROG_CPP

AC_DISABLE_STATIC
AC_PROG_LIBTOOL

# Checks for libraries.

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([stdlib.h string.h sys/time.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_C_INLINE
AC_HEADER_TIME
AC_HEADER_STDBOOL

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([gettimeofday memset strstr])

AC_CONFIG_FILES([Makefile ./src/Makefile])
AC_OUTPUT
