package org.hornetq.utils;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
* A ClassloadingUtil *
* @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
*/

public class ClassloadingUtil
{
   public static Object safeInitNewInstance(final String className)
   {
      return AccessController.doPrivileged(new PrivilegedAction<Object>()
      {
         public Object run()
         {
            ClassLoader loader = ClassloadingUtil.class.getClassLoader();
            if (loader == null)
            {
               loader = Thread.currentThread().getContextClassLoader();
            }
            try
            {
               Class<?> clazz = loader.loadClass(className);
               return clazz.newInstance();
            }
            catch (Exception e)
            {
               throw new IllegalArgumentException("Error instantiating connector factory \"" + className + "\"", e);
            }
         }
      });
   }

}

