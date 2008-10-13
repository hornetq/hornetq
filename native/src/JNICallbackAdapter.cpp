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
#include "JNICallbackAdapter.h"
#include <iostream>
#include "JavaUtilities.h"

JNICallbackAdapter::JNICallbackAdapter(AIOController * _controller, jobject _callback, jobject _fileController, jobject _bufferReference) : CallbackAdapter()
{
	controller = _controller;
	callback = _callback;
	fileController = _fileController;
	bufferReference = _bufferReference;
}

JNICallbackAdapter::~JNICallbackAdapter()
{
}

void JNICallbackAdapter::done(THREAD_CONTEXT threadContext)
{
	JNI_ENV(threadContext)->CallVoidMethod(fileController, controller->done, callback, bufferReference); 
	release(threadContext);
}

void JNICallbackAdapter::onError(THREAD_CONTEXT threadContext, long errorCode, std::string error)
{
	controller->log(threadContext, 0, "Libaio event generated errors, callback object was informed about it");
	jstring strError = JNI_ENV(threadContext)->NewStringUTF(error.data());
	JNI_ENV(threadContext)->CallVoidMethod(fileController, controller->error, callback, (jint)errorCode, strError);
	release(threadContext);
}

