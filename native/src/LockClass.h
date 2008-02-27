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

*/

#ifndef LOCKCLASS_H_
#define LOCKCLASS_H_

#include <pthread.h>

class LockClass
{
protected:
    pthread_mutex_t* _m;
public:
    inline LockClass(pthread_mutex_t* m) : _m(m)
    {
        ::pthread_mutex_lock(_m);
    }
    inline ~LockClass()
    {
        ::pthread_mutex_unlock(_m);
    }
};


#endif /*LOCKCLASS_H_*/
