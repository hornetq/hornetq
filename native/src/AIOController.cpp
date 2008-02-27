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


#include <string>
#include "AIOController.h"
#include "JavaUtilities.h"
#include "JAIODatatypes.h"

AIOController::AIOController(std::string fileName, int maxIO) : logger(0), fileOutput(fileName, this, maxIO) 
{
}

void AIOController::log(THREAD_CONTEXT threadContext, short level, char * message)
{
	jmethodID methodID = 0;
	
	switch (level)
	{
	case 0: methodID = loggerError; break;
	case 1: methodID = loggerWarn; break;
	case 2: methodID = loggerInfo; break;
	case 3: methodID = loggerDebug; break;
	default: methodID = loggerDebug; break;
	}

#ifdef DEBUG
	fprintf (stderr,"Callig log methodID=%ld, message=%s, logger=%ld, threadContext = %ld\n", (long) methodID, message, (long) logger, (long) threadContext); fflush(stderr);
#endif
	threadContext->CallVoidMethod(logger,methodID,threadContext->NewStringUTF(message));
}


void AIOController::destroy(THREAD_CONTEXT context)
{
	context->DeleteGlobalRef(logger);
}

/*
 * level = 0-error, 1-warn, 2-info, 3-debug
 */


AIOController::~AIOController()
{
}
