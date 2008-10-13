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

#ifndef BUFFERADAPTER_H_
#define BUFFERADAPTER_H_

#include <iostream>

#include "JAIODatatypes.h"

class CallbackAdapter
{
private:
    // The number of pages that are expected to be used by this Buffer
    int blocks;

    virtual void destroy(THREAD_CONTEXT threadContext) = 0;
public:
	CallbackAdapter() : blocks(1)
	{
		
	}
	virtual ~CallbackAdapter()
	{
		
	}
	
	void setBlocks(int _blocks)
	{
		blocks = _blocks;
	}
	
	void addBlock()
	{
		// TODO: Do I need to mutex here?
		blocks++;
	}
	
	void completeBlock(THREAD_CONTEXT threadContext)
	{
		// TODO: Do I need to mutex here?
		if (--blocks <= 0)
		{
			done(threadContext);
		}
	}
	
	virtual void addref(THREAD_CONTEXT ) = 0;
	virtual void deleteRef(THREAD_CONTEXT ) = 0;
	virtual void done(THREAD_CONTEXT ) = 0;
	virtual void onError(THREAD_CONTEXT , long , std::string )=0;
};
#endif /*BUFFERADAPTER_H_*/
