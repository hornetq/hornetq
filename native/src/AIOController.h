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


#ifndef AIOCONTROLLER_H_
#define AIOCONTROLLER_H_
#include <jni.h>
#include <string>
#include "JAIODatatypes.h"
#include "AsyncFile.h"

class AIOController
{
public:
	jmethodID done;
	jmethodID error;

	jobject logger;
	
	jmethodID loggerError;
	jmethodID loggerWarn;
	jmethodID loggerDebug;
	jmethodID loggerInfo;

	/*
	 * level = 0-error, 1-warn, 2-info, 3-debug
	 */
	void log(THREAD_CONTEXT threadContext, short level, const char * message);
	
	AsyncFile fileOutput;
	
	void destroy(THREAD_CONTEXT context);
	
	AIOController(std::string fileName, int maxIO);
	virtual ~AIOController();
};
#endif /*AIOCONTROLLER_H_*/
