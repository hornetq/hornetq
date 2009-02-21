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

#ifndef JAVAUTILITIES_H_
#define JAVAUTILITIES_H_
#include <string>
#include <jni.h>

void throwException(JNIEnv * env, const int code, const char * message);
std::string convertJavaString(JNIEnv * env, jstring& jstr);

#endif /*JAVAUTILITIES_H_*/
