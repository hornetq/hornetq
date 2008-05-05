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

#ifndef JNIBUFFERADAPTER_H_
#define JNIBUFFERADAPTER_H_

#include <iostream>

#include "CallbackAdapter.h"
#include "AIOController.h"
#include "JAIODatatypes.h"


class JNICallbackAdapter : public CallbackAdapter
{
private:
	AIOController * controller;
	jobject callback;
	jobject fileController;
	int refs;
    void destroy(THREAD_CONTEXT threadContext);
	
public:
	// _ob must be a global Reference (use createGloblReferente before calling the constructor)
	JNICallbackAdapter(AIOController * _controller, jobject _callback, jobject _fileController);
	virtual ~JNICallbackAdapter();
	void done(THREAD_CONTEXT threadContext);
	void onError(THREAD_CONTEXT threadContext, long error, std::string error);
	
	void addref(THREAD_CONTEXT )
	{
		// As long as there is only one thread polling events, we are safe with this
		refs++;
	}
	
	void deleteRef(THREAD_CONTEXT threadContext)
	{
		// As long as there is only one thread polling events, we are safe with this
		if (--refs <= 0)
		{
			destroy(threadContext);
			delete this;
		}
	}
	
	
};
#endif /*JNIBUFFERADAPTER_H_*/
