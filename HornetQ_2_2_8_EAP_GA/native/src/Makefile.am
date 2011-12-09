#AM_CXXFLAGS = $(JVM_CXXFLAGS)
lib_LTLIBRARIES = libHornetQAIO.la
bin_PROGRAMS = disktest

libHornetQAIO_la_SOURCES = JNI_AsynchronousFileImpl.cpp AIOController.cpp AIOController.h AIOException.h AsyncFile.cpp \
                     AsyncFile.h CallbackAdapter.h JAIODatatypes.h JavaUtilities.cpp \
                     JavaUtilities.h JNICallbackAdapter.cpp JNICallbackAdapter.h \
                     LockClass.h org_hornetq_core_persistence_impl_libaio_jni_impl_AsynchronousFileImpl.h \
                     Version.h

disktest_SOURCES = disktest.cpp

