/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <jni.h>
#include <stdlib.h>
#include <iostream>
#include <stdio.h>
#include <fcntl.h>
#include <string>
#include <time.h>


#include "org_hornetq_core_asyncio_impl_AsynchronousFileImpl.h"


#include "JavaUtilities.h"
#include "AIOController.h"
#include "JNICallbackAdapter.h"
#include "AIOException.h"
#include "Version.h"


// This value is set here globally, to avoid passing stuff on stack between java and the native layer on every sleep call
struct timespec nanoTime;


/*
 * Class:     org_jboss_jaio_libaioimpl_LibAIOController
 * Method:    init
 * Signature: (Ljava/lang/String;Ljava/lang/Class;)J
 */
JNIEXPORT jlong JNICALL Java_org_hornetq_core_asyncio_impl_AsynchronousFileImpl_init
  (JNIEnv * env, jclass clazz, jstring jstrFileName, jint maxIO, jobject logger)
{
	AIOController * controller = 0;
	try
	{
		std::string fileName = convertJavaString(env, jstrFileName);

		controller = new AIOController(fileName, (int) maxIO);
		controller->done = env->GetMethodID(clazz,"callbackDone","(Lorg/hornetq/core/asyncio/AIOCallback;JLjava/nio/ByteBuffer;)V");
		if (!controller->done) return 0;

		controller->error = env->GetMethodID(clazz, "callbackError", "(Lorg/hornetq/core/asyncio/AIOCallback;JLjava/nio/ByteBuffer;ILjava/lang/String;)V");
        if (!controller->error) return 0;

        jclass loggerClass = env->GetObjectClass(logger);

        if (!(controller->loggerDebug = env->GetMethodID(loggerClass, "debug", "(Ljava/lang/Object;)V"))) return 0;
        if (!(controller->loggerWarn = env->GetMethodID(loggerClass, "warn", "(Ljava/lang/Object;)V"))) return 0;
        if (!(controller->loggerInfo = env->GetMethodID(loggerClass, "info", "(Ljava/lang/Object;)V"))) return 0;
        if (!(controller->loggerError = env->GetMethodID(loggerClass, "error", "(Ljava/lang/Object;)V"))) return 0;

        controller->logger = env->NewGlobalRef(logger);

//        controller->log(env,4, "Controller initialized");

	    return (jlong)controller;
	}
	catch (AIOException& e){
		if (controller != 0)
		{
			delete controller;
		}
		throwException(env, e.getErrorCode(), e.what());
		return 0;
	}
}

JNIEXPORT void JNICALL Java_org_hornetq_core_asyncio_impl_AsynchronousFileImpl_read
  (JNIEnv *env, jobject objThis, jlong controllerAddress, jlong position, jlong size, jobject jbuffer, jobject callback)
{
	try
	{
		AIOController * controller = (AIOController *) controllerAddress;
		void * buffer = env->GetDirectBufferAddress(jbuffer);

		if (buffer == 0)
		{
			throwException(env, NATIVE_ERROR_INVALID_BUFFER, "Invalid Buffer used, libaio requires NativeBuffer instead of Java ByteBuffer");
			return;
		}

		if (((long)buffer) % 512)
		{
			throwException(env, NATIVE_ERROR_NOT_ALIGNED, "Buffer not aligned for use with DMA");
			return;
		}

		CallbackAdapter * adapter = new JNICallbackAdapter(controller, -1, env->NewGlobalRef(callback), env->NewGlobalRef(objThis), env->NewGlobalRef(jbuffer), true);

		controller->fileOutput.read(env, position, (size_t)size, buffer, adapter);
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
	}
}


// Fast memset on buffer
JNIEXPORT void JNICALL Java_org_hornetq_core_asyncio_impl_AsynchronousFileImpl_resetBuffer
  (JNIEnv *env, jclass, jobject jbuffer, jint size)
{
	void * buffer = env->GetDirectBufferAddress(jbuffer);

	if (buffer == 0)
	{
		throwException(env, NATIVE_ERROR_INVALID_BUFFER, "Invalid Buffer used, libaio requires NativeBuffer instead of Java ByteBuffer");
		return;
	}

	memset(buffer, 0, (size_t)size);

}

JNIEXPORT void JNICALL Java_org_hornetq_core_asyncio_impl_AsynchronousFileImpl_destroyBuffer
  (JNIEnv * env, jclass, jobject jbuffer)
{
	void *  buffer = env->GetDirectBufferAddress(jbuffer);
	free(buffer);
}

JNIEXPORT jobject JNICALL Java_org_hornetq_core_asyncio_impl_AsynchronousFileImpl_newNativeBuffer
  (JNIEnv * env, jclass, jlong size)
{
	try
	{

		if (size % ALIGNMENT)
		{
			throwException(env, NATIVE_ERROR_INVALID_BUFFER, "Buffer size needs to be aligned to 512");
			return 0;
		}


		// This will allocate a buffer, aligned by 512.
		// Buffers created here need to be manually destroyed by destroyBuffer, or this would leak on the process heap away of Java's GC managed memory
		void * buffer = 0;
		if (::posix_memalign(&buffer, 512, size))
		{
			throwException(env, NATIVE_ERROR_INTERNAL, "Error on posix_memalign");
			return 0;
		}

		memset(buffer, 0, (size_t)size);

		jobject jbuffer = env->NewDirectByteBuffer(buffer, size);
		return jbuffer;
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
		return 0;
	}
}

JNIEXPORT void JNICALL Java_org_hornetq_core_asyncio_impl_AsynchronousFileImpl_write
  (JNIEnv *env, jobject objThis, jlong controllerAddress, jlong sequence, jlong position, jlong size, jobject jbuffer, jobject callback)
{
	try
	{
		AIOController * controller = (AIOController *) controllerAddress;
		void * buffer = env->GetDirectBufferAddress(jbuffer);

		if (buffer == 0)
		{
			throwException(env, NATIVE_ERROR_INVALID_BUFFER, "Invalid Buffer used, libaio requires NativeBuffer instead of Java ByteBuffer");
			return;
		}


		CallbackAdapter * adapter = new JNICallbackAdapter(controller, sequence, env->NewGlobalRef(callback), env->NewGlobalRef(objThis), env->NewGlobalRef(jbuffer), false);

		controller->fileOutput.write(env, position, (size_t)size, buffer, adapter);
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
	}
}



JNIEXPORT void Java_org_hornetq_core_asyncio_impl_AsynchronousFileImpl_internalPollEvents
  (JNIEnv *env, jclass, jlong controllerAddress)
{
	try
	{
		AIOController * controller = (AIOController *) controllerAddress;
		controller->fileOutput.pollEvents(env);
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
	}
}

JNIEXPORT void JNICALL Java_org_hornetq_core_asyncio_impl_AsynchronousFileImpl_stopPoller
  (JNIEnv *env, jclass, jlong controllerAddress)
{
	try
	{
		AIOController * controller = (AIOController *) controllerAddress;
		controller->fileOutput.stopPoller(env);
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
	}
}

JNIEXPORT void JNICALL Java_org_hornetq_core_asyncio_impl_AsynchronousFileImpl_closeInternal
  (JNIEnv *env, jclass, jlong controllerAddress)
{
	try
	{
		if (controllerAddress != 0)
		{
			AIOController * controller = (AIOController *) controllerAddress;
			controller->destroy(env);
			delete controller;
		}
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
	}
}


JNIEXPORT void JNICALL Java_org_hornetq_core_asyncio_impl_AsynchronousFileImpl_fill
  (JNIEnv * env, jclass, jlong controllerAddress, jlong position, jint blocks, jlong size, jbyte fillChar)
{
	try
	{
		AIOController * controller = (AIOController *) controllerAddress;

		controller->fileOutput.preAllocate(env, position, blocks, size, fillChar);

	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
	}
}



/** It does nothing... just return true to make sure it has all the binary dependencies */
JNIEXPORT jint JNICALL Java_org_hornetq_core_asyncio_impl_AsynchronousFileImpl_getNativeVersion
  (JNIEnv *, jclass)

{
     return _VERSION_NATIVE_AIO;
}


JNIEXPORT jlong JNICALL Java_org_hornetq_core_asyncio_impl_AsynchronousFileImpl_size0
  (JNIEnv * env, jobject, jlong controllerAddress)
{
	try
	{
		AIOController * controller = (AIOController *) controllerAddress;

		long size = controller->fileOutput.getSize();
		if (size < 0)
		{
			throwException(env, NATIVE_ERROR_INTERNAL, "InternalError on Native Layer: method size failed");
			return -1l;
		}
		return size;
	}
	catch (AIOException& e)
	{
		throwException(env, e.getErrorCode(), e.what());
		return -1l;
	}

}

/*
 * Class:     org_hornetq_core_asyncio_impl_AsynchronousFileImpl
 * Method:    setNanoSleepInterval
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_hornetq_core_asyncio_impl_AsynchronousFileImpl_setNanoSleepInterval
  (JNIEnv *, jclass, jint nanotime)
{
	nanoTime.tv_sec = 0;
	nanoTime.tv_nsec = (long)nanotime;
}

/*
 * Class:     org_hornetq_core_asyncio_impl_AsynchronousFileImpl
 * Method:    nanoSleep
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_org_hornetq_core_asyncio_impl_AsynchronousFileImpl_nanoSleep
  (JNIEnv *, jclass)
{
	if (nanoTime.tv_nsec != 0)
	{
		nanosleep(&nanoTime, 0);
	}
}

