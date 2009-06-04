#AM_CXXFLAGS = $(JVM_CXXFLAGS)
lib_LTLIBRARIES = libJBMLibAIO.la
bin_PROGRAMS = disktest

libJBMLibAIO_la_SOURCES = JNI_AsynchronousFileImpl.cpp AIOController.cpp AIOController.h AIOException.h AsyncFile.cpp \
                     AsyncFile.h CallbackAdapter.h JAIODatatypes.h JavaUtilities.cpp \
                     JavaUtilities.h JNICallbackAdapter.cpp JNICallbackAdapter.h \
                     LockClass.h org_jboss_messaging_core_persistence_impl_libaio_jni_impl_AsynchronousFileImpl.h \
                     Version.h

disktest_SOURCES = disktest.cpp

