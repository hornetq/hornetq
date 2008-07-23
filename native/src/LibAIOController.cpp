/*
    Copyright (C) 2008 Red Hat Software - JBoss Middleware Division


    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
    USA

    The GNU Lesser General Public License is available in the file COPYING.
    
    Software written by Clebert Suconic (csuconic at redhat dot com)
*/

#include <jni.h>
#include <stdlib.h>
#include <iostream>
#include <stdio.h>
#include <fcntl.h>
#include <string>


#include "org_jboss_messaging_core_asyncio_impl_AsynchronousFileImpl.h"


#include "JavaUtilities.h"
#include "AIOController.h"
#include "JNICallbackAdapter.h"
#include "AIOException.h"




/*
 * Class:     org_jboss_jaio_libaioimpl_LibAIOController
 * Method:    init
 * Signature: (Ljava/lang/String;Ljava/lang/Class;)J
 */
JNIEXPORT jlong JNICALL Java_org_jboss_messaging_core_asyncio_impl_AsynchronousFileImpl_init
  (JNIEnv * env, jclass clazz, jstring jstrFileName, jint maxIO, jobject logger)
{
	try
	{
		std::string fileName = convertJavaString(env, jstrFileName);
		
		AIOController * controller = new AIOController(fileName, (int) maxIO);
		controller->done = env->GetMethodID(clazz,"callbackDone","(Lorg/jboss/messaging/core/asyncio/AIOCallback;)V");
		if (!controller->done) return 0;
		
		controller->error = env->GetMethodID(clazz, "callbackError", "(Lorg/jboss/messaging/core/asyncio/AIOCallback;ILjava/lang/String;)V");
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
		throwException(env, "java/lang/RuntimeException", e.what());
		return 0;
	}
}

JNIEXPORT void JNICALL Java_org_jboss_messaging_core_asyncio_impl_AsynchronousFileImpl_read
  (JNIEnv *env, jobject objThis, jlong controllerAddress, jlong position, jlong size, jobject jbuffer, jobject callback)
{
	try 
	{
		AIOController * controller = (AIOController *) controllerAddress;
		void * buffer = env->GetDirectBufferAddress(jbuffer);
		
		if (buffer == 0)
		{
			throwException(env, "java/lang/IllegalStateException", "Invalid Direct Buffer used");
			return;
		}
		
		if (((long)buffer) % 512)
		{
			throwException(env, "java/lang/IllegalStateException", "Buffer not aligned for use with DMA");
			return;
		}
		
		CallbackAdapter * adapter = new JNICallbackAdapter(controller, env->NewGlobalRef(callback), env->NewGlobalRef(objThis), env->NewGlobalRef(jbuffer));
		
		controller->fileOutput.read(env, position, (size_t)size, buffer, adapter);
	}
	catch (AIOException& e)
	{
		throwException(env, "java/lang/RuntimeException", e.what());
	}
}

JNIEXPORT void JNICALL Java_org_jboss_messaging_core_asyncio_impl_AsynchronousFileImpl_write
  (JNIEnv *env, jobject objThis, jlong controllerAddress, jlong position, jlong size, jobject jbuffer, jobject callback)
{
	try 
	{
		AIOController * controller = (AIOController *) controllerAddress;
		void * buffer = env->GetDirectBufferAddress(jbuffer);
		if (buffer == 0)
		{
			throwException(env, "java/lang/IllegalStateException", "Invalid Direct Buffer used");
			return;
		}
		
		CallbackAdapter * adapter = new JNICallbackAdapter(controller, env->NewGlobalRef(callback), env->NewGlobalRef(objThis), env->NewGlobalRef(jbuffer));
		
		controller->fileOutput.write(env, position, (size_t)size, buffer, adapter);
	}
	catch (AIOException& e)
	{
		throwException(env, "java/lang/RuntimeException", e.what());
	}
}



JNIEXPORT void Java_org_jboss_messaging_core_asyncio_impl_AsynchronousFileImpl_internalPollEvents
  (JNIEnv *env, jclass, jlong controllerAddress)
{
	try
	{
		AIOController * controller = (AIOController *) controllerAddress;
		controller->fileOutput.pollEvents(env);
	}
	catch (AIOException& e)
	{
		throwException(env, "java/lang/RuntimeException", e.what());
	}
}

JNIEXPORT jobject JNICALL Java_org_jboss_messaging_core_asyncio_impl_AsynchronousFileImpl_newBuffer
  (JNIEnv * env, jobject, jlong size)
{
	try
	{
		
		if (size % ALIGNMENT)
		{
			throwException(env, "java/lang/RuntimeException", "Buffer size needs to be aligned to 512");
			return 0;
		}
		
		void * buffer = 0;
		if (::posix_memalign(&buffer, 512, size))
		{
			throw AIOException(10, "Error on posix_memalign");
		}
		return env->NewDirectByteBuffer(buffer, size);
	}
	catch (AIOException& e)
	{
		throwException(env, "java/lang/RuntimeException", e.what());
		return 0;
	}
}

JNIEXPORT void JNICALL Java_org_jboss_messaging_core_asyncio_impl_AsynchronousFileImpl_destroyBuffer
  (JNIEnv * env, jobject, jobject jbuffer)
{
	void *  buffer = env->GetDirectBufferAddress(jbuffer);
	free(buffer);
}


JNIEXPORT void JNICALL Java_org_jboss_messaging_core_asyncio_impl_AsynchronousFileImpl_stopPoller
  (JNIEnv *env, jclass, jlong controllerAddress)
{
	try
	{
		AIOController * controller = (AIOController *) controllerAddress;
		controller->fileOutput.stopPoller(env);
	}
	catch (AIOException& e)
	{
		throwException(env, "java/lang/RuntimeException", e.what());
	}
}

JNIEXPORT void JNICALL Java_org_jboss_messaging_core_asyncio_impl_AsynchronousFileImpl_closeInternal
  (JNIEnv *env, jclass, jlong controllerAddress)
{
	try
	{
		AIOController * controller = (AIOController *) controllerAddress;
		controller->destroy(env);
		delete controller;
	}
	catch (AIOException& e)
	{
		throwException(env, "java/lang/RuntimeException", e.what());
	}
}


JNIEXPORT void JNICALL Java_org_jboss_messaging_core_asyncio_impl_AsynchronousFileImpl_fill
  (JNIEnv * env, jclass, jlong controllerAddress, jlong position, jint blocks, jlong size, jbyte fillChar)
{
	try
	{
		AIOController * controller = (AIOController *) controllerAddress;
		
		controller->fileOutput.preAllocate(env, position, blocks, size, fillChar);
		
		//controller->fileOutput.preAllocate(env, blocks, size);
	}
	catch (AIOException& e)
	{
		throwException(env, "java/lang/RuntimeException", e.what());
	}
}

/** It does nothing... just return true to make sure it has all the binary dependencies */
JNIEXPORT jboolean JNICALL Java_org_jboss_messaging_core_asyncio_impl_AsynchronousFileImpl_isNativeLoaded
  (JNIEnv *, jclass)
{
	return 1;
}
