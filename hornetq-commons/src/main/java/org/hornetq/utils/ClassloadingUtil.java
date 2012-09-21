package org.hornetq.utils;

import java.lang.reflect.Constructor;
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

   public static Object newInstanceFromClassLoader(final String className, Object... objs)
   {
      ClassLoader loader = ClassloadingUtil.class.getClassLoader();
      try
      {
         Class<?>[] parametersType  = new Class<?>[objs.length];
         for (int i = 0 ; i < objs.length; i++)
         {
            parametersType[i] = objs[i].getClass();
         }
         Class<?> clazz = loader.loadClass(className);
         return clazz.getConstructor(parametersType).newInstance(objs);
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

   /**
    * This method can load a class and instantiate it with a constructor that takes actual parameters whose types are
    * any subclasses of their corresponding declared type, using reflection technique. 
    * For example, if a class Foo has a constructor that takes one parameter of type Color:
    * <code>
    * public class Foo
    * {
    *    public Foo(Color a) {}
    * }
    * </code>
    * Then you create a Blue object b (Blue extends Color) and you want to use that to create a Foo (new Foo(b)) 
    * using reflection, you can call this method. like
    * 
    * Foo foo = (Foo)newInstanceFromClassLoaderWithSubClassParameters("Foo", b);
    * 
    * @param className
    * @param objs
    * @return
    */
   public static Object newInstanceFromClassLoaderWithSubClassParameters(final String className, Object... objs)
   {
      ClassLoader loader = ClassloadingUtil.class.getClassLoader();
      try
      {
         Class<?> clazz = loader.loadClass(className);
         Constructor<?>[] constructors = clazz.getConstructors();
         Constructor<?> found = null;
         for (Constructor<?> cotr : constructors)
         {
            Class<?>[] paramTypes = cotr.getParameterTypes();
            if (paramTypes.length == objs.length)
            {
               boolean match = true;
               for (int i = 0; i < objs.length; i++)
               {
                  if (!paramTypes[i].isInstance(objs[i]))
                  {
                     match = false;
                     break;
                  }
                  objs[i] = paramTypes[i].cast(objs[i]);
               }
               if (match)
               {
                  found = cotr;
                  break;
               }
            }
         }
         if (found == null)
         {
            throw new InstantiationException("No suitable constructor found for " + className);
         }
         return found.newInstance(objs);
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
