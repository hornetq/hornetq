package org.hornetq.utils;

import java.net.URL;

/**
* This class will be used to perform generic class-loader operations,
* such as load a class first using TCCL, and then the classLoader used by HornetQ (ClassloadingUtil.getClass().getClassLoader()).
* 
* Is't required to use a Security Block on any calls to this class.
* 
* @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
*/

public class ClassloadingUtil
{
   public static Object newInstanceFromClassLoader(final String className)
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

   public static URL findResource(final String resourceName)
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

}
