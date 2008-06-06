/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import junit.framework.TestCase;

import org.jboss.messaging.util.ObjectInputStreamWithClassLoader;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class ObjectInputStreamWithClassLoaderTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static ClassLoader newClassLoader(Class anyUserClass) throws Exception
   {
      URL classLocation = anyUserClass.getProtectionDomain().getCodeSource().getLocation();
      StringTokenizer tokenString = new StringTokenizer(System.getProperty("java.class.path"),
         File.pathSeparator);
      String pathIgnore = System.getProperty("java.home");
      if (pathIgnore == null)
      {
         pathIgnore = classLocation.toString();
      }

      List<URL> urls = new ArrayList<URL>();
      while (tokenString.hasMoreElements())
      {
         String value = tokenString.nextToken();
         URL itemLocation = new File(value).toURL();
         if (!itemLocation.equals(classLocation) &&
                      itemLocation.toString().indexOf(pathIgnore) >= 0)
         {
            urls.add(itemLocation);
         }
      }

      URL[] urlArray = (URL[]) urls.toArray(new URL[urls.size()]);

      ClassLoader masterClassLoader = URLClassLoader.newInstance(urlArray, null);
      ClassLoader appClassLoader = URLClassLoader.newInstance(new URL[]{classLocation},
                                      masterClassLoader);
      return appClassLoader;
   }
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testClassLoaderIsolation() throws Exception
   {

      ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
      try
      {
         AnObject obj = new AnObject();
         byte[] bytes = toBytes(obj);
         
         ClassLoader testClassLoader = newClassLoader(obj.getClass());
         Thread.currentThread().setContextClassLoader(testClassLoader);

         ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         ObjectInputStreamWithClassLoader ois = new ObjectInputStreamWithClassLoader(bais);

         Object deserializedObj= ois.readObject();
         
         assertNotSame(obj, deserializedObj);
         assertNotSame(obj.getClass(), deserializedObj.getClass());
         assertNotSame(obj.getClass().getClassLoader(),
               deserializedObj.getClass().getClassLoader());
         assertSame(testClassLoader,
               deserializedObj.getClass().getClassLoader());
      }
      finally
      {
         Thread.currentThread().setContextClassLoader(originalClassLoader);
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static byte[] toBytes(Object obj) throws IOException
   {
      assertTrue(obj instanceof Serializable);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      oos.flush();
      return baos.toByteArray();    
   }
   
   // Inner classes -------------------------------------------------
   
   private static class AnObject implements Serializable
   {
      private static final long serialVersionUID = -5172742084489525256L;      
   }
}
