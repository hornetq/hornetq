package org.hornetq.utils;

import java.net.URL;
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
            try
            {
               Class<?> clazz = loader.loadClass(className);
               return clazz.newInstance();
            }
            catch (Throwable t)
            {
                try
                {
                    loader = Thread.currentThread().getContextClassLoader();
                    if (loader != null)
                        return loader.loadClass(className).newInstance();
                }
                catch (RuntimeException e)
                {
                    throw e;
                }
                catch (Exception e)
                {
                }

                throw new IllegalArgumentException("Could not find class " + className);
            }
         }
      });
   }

   public static URL findResource(final String resourceName)
   {
      return AccessController.doPrivileged(new PrivilegedAction<URL>()
      {
         public URL run()
         {
            ClassLoader loader = ClassloadingUtil.class.getClassLoader();
            try
            {
               URL resource = loader.getResource(resourceName);
               if (resource != null)
                   return resource;
            }
            catch (Throwable t)
            {
            }

            loader = Thread.currentThread().getContextClassLoader();
            if (loader == null)
                return null;

            URL resource = loader.getResource(resourceName);
            if (resource != null)
                return resource;

             return null;
         }
      });
   }

}

