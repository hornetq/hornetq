/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.unit.util;

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

import junit.framework.Assert;

import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.ObjectInputStreamWithClassLoader;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:csuconic@redhat.com">Clebert Suconic</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class ObjectInputStreamWithClassLoaderTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static ClassLoader newClassLoader(final Class anyUserClass) throws Exception
   {
      URL classLocation = anyUserClass.getProtectionDomain().getCodeSource().getLocation();
      StringTokenizer tokenString = new StringTokenizer(System.getProperty("java.class.path"), File.pathSeparator);
      String pathIgnore = System.getProperty("java.home");
      if (pathIgnore == null)
      {
         pathIgnore = classLocation.toString();
      }

      List<URL> urls = new ArrayList<URL>();
      while (tokenString.hasMoreElements())
      {
         String value = tokenString.nextToken();
         URL itemLocation = new File(value).toURI().toURL();
         if (!itemLocation.equals(classLocation) && itemLocation.toString().indexOf(pathIgnore) >= 0)
         {
            urls.add(itemLocation);
         }
      }

      URL[] urlArray = urls.toArray(new URL[urls.size()]);

      ClassLoader masterClassLoader = URLClassLoader.newInstance(urlArray, null);
      ClassLoader appClassLoader = URLClassLoader.newInstance(new URL[] { classLocation }, masterClassLoader);
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
         byte[] bytes = ObjectInputStreamWithClassLoaderTest.toBytes(obj);

         ClassLoader testClassLoader = ObjectInputStreamWithClassLoaderTest.newClassLoader(obj.getClass());
         Thread.currentThread().setContextClassLoader(testClassLoader);

         ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         org.hornetq.utils.ObjectInputStreamWithClassLoader ois = new ObjectInputStreamWithClassLoader(bais);

         Object deserializedObj = ois.readObject();

         Assert.assertNotSame(obj, deserializedObj);
         Assert.assertNotSame(obj.getClass(), deserializedObj.getClass());
         Assert.assertNotSame(obj.getClass().getClassLoader(), deserializedObj.getClass().getClassLoader());
         Assert.assertSame(testClassLoader, deserializedObj.getClass().getClassLoader());
      }
      finally
      {
         Thread.currentThread().setContextClassLoader(originalClassLoader);
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static byte[] toBytes(final Object obj) throws IOException
   {
      Assert.assertTrue(obj instanceof Serializable);
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
