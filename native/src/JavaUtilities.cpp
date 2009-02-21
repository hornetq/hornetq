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

#include <stdio.h>
#include <iostream>
#include <string>
#include "JavaUtilities.h"


void throwRuntimeException(JNIEnv * env, const char * message)
{
  jclass exceptionClass = env->FindClass("java/lang/RuntimeException");
  env->ThrowNew(exceptionClass,message);
  
}

void throwException(JNIEnv * env, const int code, const char * message)
{
  jclass exceptionClass = env->FindClass("org/jboss/messaging/core/exception/MessagingException");
  if (exceptionClass==NULL) 
  {
     std::cerr << "Couldn't throw exception message:= " << message << "\n";
     throwRuntimeException (env, "Can't find Exception class");
     return;
  }

  jmethodID constructor = env->GetMethodID(exceptionClass, "<init>", "(ILjava/lang/String;)V");
  if (constructor == NULL)
  {
       std::cerr << "Couldn't find the constructor ***";
       throwRuntimeException (env, "Can't find Constructor for Exception");
       return;
  }

  jstring strError = env->NewStringUTF(message);
  jthrowable ex = (jthrowable)env->NewObject(exceptionClass, constructor, code, strError);
  env->Throw(ex);
  
}

std::string convertJavaString(JNIEnv * env, jstring& jstr)
{
	const char * valueStr = env->GetStringUTFChars(jstr, NULL);
	std::string data(valueStr);
	env->ReleaseStringUTFChars(jstr, valueStr);
	return data;
}

