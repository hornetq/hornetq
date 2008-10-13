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

public:
	CallbackAdapter()
	{
		
	}
	virtual ~CallbackAdapter()
	{
		
	}
	
	virtual void done(THREAD_CONTEXT ) = 0;
	virtual void onError(THREAD_CONTEXT , long , std::string )=0;
};
#endif /*BUFFERADAPTER_H_*/
