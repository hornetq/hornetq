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

void throwException(JNIEnv * env,const char * clazz, const char * message)
{
  jclass exceptionClass = env->FindClass(clazz);
  if (exceptionClass==NULL) 
  {
     exceptionClass = env->FindClass("java/lang/RuntimeException");
     if (exceptionClass==NULL) 
     {
    	std::cerr << "Couldn't throw exception " << clazz << " message:= " << message << "\n";
        return;
     }
  }
  
  env->ThrowNew(exceptionClass,message);
  
}

std::string convertJavaString(JNIEnv * env, jstring& jstr)
{
	const char * valueStr = env->GetStringUTFChars(jstr, NULL);
	std::string data(valueStr);
	//data+=valueStr;
	env->ReleaseStringUTFChars(jstr, valueStr);
	return data;
}

