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

JNICallbackAdapter::JNICallbackAdapter(AIOController * _controller, jobject _obj) : CallbackAdapter(), refs(1)
{
	controller = _controller;
	obj = _obj;
}

JNICallbackAdapter::~JNICallbackAdapter()
{
}

void JNICallbackAdapter::done(THREAD_CONTEXT threadContext)
{
	JNI_ENV(threadContext)->CallVoidMethod(obj,controller->done); 
	return;
}

void JNICallbackAdapter::onError(THREAD_CONTEXT threadContext, long errorCode, std::string error)
{
	controller->log(threadContext, 0, "Libaio event generated errors, callback object was informed about it");
	jstring strError = JNI_ENV(threadContext)->NewStringUTF(error.data());
	JNI_ENV(threadContext)->CallVoidMethod(obj, controller->error, (jint)errorCode, strError);
}

void JNICallbackAdapter::destroy(THREAD_CONTEXT threadContext)
{
	JNI_ENV(threadContext)->DeleteGlobalRef(obj);
}
