/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.tests.unit.util;

import junit.framework.TestCase;
import org.jboss.messaging.util.ObjectInputStreamWithClassLoader;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:csuconic@redhat.com">Clebert Suconic</a>
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
