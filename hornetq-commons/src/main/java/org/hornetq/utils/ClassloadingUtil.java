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
   private static final String INSTANTIATION_EXCEPTION_MESSAGE =
            "Your class must have a constructor without arguments. If it is an inner class, it must be static!";

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
         if (t instanceof InstantiationException)
         {
            System.out.println(INSTANTIATION_EXCEPTION_MESSAGE);
         }
         loader = Thread.currentThread().getContextClassLoader();
         if (loader == null)
            throw new RuntimeException("No local context classloader", t);

         try
         {
            return loader.loadClass(className).newInstance();
         }
         catch (InstantiationException e)
         {
            throw new RuntimeException(INSTANTIATION_EXCEPTION_MESSAGE + " " + className, e);
         }
         catch (ClassNotFoundException e)
         {
            throw new IllegalStateException(e);
         }
         catch (IllegalAccessException e)
         {
            throw new RuntimeException(e);
         }
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

      return loader.getResource(resourceName);
   }
}
