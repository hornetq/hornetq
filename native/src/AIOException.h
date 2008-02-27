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



#ifndef AIOEXCEPTION_H_
#define AIOEXCEPTION_H_

#include <exception>
#include <string>

class AIOException : public std::exception
{
private:
	int errorCode;
	std::string message;
public:
	AIOException(int _errorCode, std::string  _message) throw() : errorCode(_errorCode), message(_message)
	{
		errorCode = _errorCode;
		message = _message;
	}
	
	AIOException(int _errorCode, const char * _message) throw ()
	{
		message = std::string(_message);
		errorCode = _errorCode;
	}
	
	virtual ~AIOException() throw()
	{
		
	}
	
	int inline getErrorCode()
	{
		return errorCode;
	}
	
    const char* what() const throw()
    {
    	return message.data();
    }
	
};

#endif /*AIOEXCEPTION_H_*/
