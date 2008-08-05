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

#ifndef FILEOUTPUT_H_
#define FILEOUTPUT_H_

#include <string>
#include <libaio.h>
#include <pthread.h>
#include "JAIODatatypes.h"
#include "AIOException.h"

class AIOController;

class CallbackAdapter;

class AsyncFile
{
private:
	io_context_t aioContext;
	struct io_event *events; 
	int fileHandle;
	std::string fileName;
	
	pthread_mutex_t fileMutex;
	pthread_mutex_t pollerMutex;
	
	AIOController * controller;
	
	bool pollerRunning;
	
	int maxIO;
	
public:
	AsyncFile(std::string & _fileName, AIOController * controller, int maxIO);
	virtual ~AsyncFile();
	
	void write(THREAD_CONTEXT threadContext, long position, size_t size, void *& buffer, CallbackAdapter *& adapter);
	
	void read(THREAD_CONTEXT threadContext, long position, size_t size, void *& buffer, CallbackAdapter *& adapter);
	
	int getHandle()
	{
		return fileHandle;
	}

	long getSize();

	inline void * newBuffer(int size)
	{
		void * buffer = 0;
		if (::posix_memalign(&buffer, 512, size))
		{
			throw AIOException(10, "Error on posix_memalign");
		}
		return buffer;
		
	}

	inline void destroyBuffer(void * buffer)
	{
		::free(buffer);
	}

	
	// Finishes the polling thread (if any) and return
	void stopPoller(THREAD_CONTEXT threadContext);
	void preAllocate(THREAD_CONTEXT threadContext, off_t position, int blocks, size_t size, int fillChar);
	
	void pollEvents(THREAD_CONTEXT threadContext);
	
};

#endif /*FILEOUTPUT_H_*/
