# serial 3
# Find valid warning flags for the C Compiler.           -*-Autoconf-*-
dnl Copyright (C) 2001, 2002, 2006 Free Software Foundation, Inc.
dnl This file is free software; the Free Software Foundation
dnl gives unlimited permission to copy and/or distribute it,
dnl with or without modifications, as long as this notice is preserved.
dnl Written by Jesse Thilo.

AC_DEFUN([gl_COMPILER_FLAGS],
  [AC_MSG_CHECKING(whether compiler accepts $1)
   AC_SUBST(COMPILER_FLAGS)
   ac_save_CFLAGS="$CFLAGS"
   CFLAGS="$CFLAGS $1"
   ac_save_CXXFLAGS="$CXXFLAGS"
   CXXFLAGS="$CXXFLAGS $1"
   AC_TRY_COMPILE(,
    [int x;],
    COMPILER_FLAGS="$COMPILER_FLAGS $1"
    AC_MSG_RESULT(yes),
    AC_MSG_RESULT(no))
  CFLAGS="$ac_save_CFLAGS"
  CXXFLAGS="$ac_save_CXXFLAGS"
 ])


AC_DEFUN([AC_JNI_INCLUDE_DIR],
[
   JNI_INCLUDE_DIRS=""

test -f $JAVA_HOME/include/jni.h || \
     AC_MSG_ERROR([JAVA_HOME=$JAVA_HOME does not appear to be a valid java home.])


JNI_INCLUDE_DIRS="$JAVA_HOME/include"
case "$OSTYPE" in
bsdi*)                  _JNI_INC_SUBDIRS="dos";;
linux*)                 _JNI_INC_SUBDIRS="linux";;
mingw32*|cygwin*)       _JNI_INC_SUBDIRS="win32";;
osf*)                   _JNI_INC_SUBDIRS="alpha";;
solaris*)               _JNI_INC_SUBDIRS="solaris";;
*)                      _JNI_INC_SUBDIRS="genunix";;
esac

# add any subdirectories that are present
for JINCSUBDIR in $_JNI_INC_SUBDIRS
do
       echo tttt $JINCSUBDIR
        if test -d "$JAVA_HOME/include/$JINCSUBDIR"; then
                JNI_INCLUDE_DIRS="$JNI_INCLUDE_DIRS $JAVA_HOME/include/$JINCSUBDIR"
        fi
done

# add it to the CPPFLAGS, for convenience
for JNI_INCLUDE_DIR in $JNI_INCLUDE_DIRS
do
        CPPFLAGS="$CPPFLAGS -I$JNI_INCLUDE_DIR"
done
])
