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

package org.hornetq.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class ObjectInputStreamWithClassLoader extends ObjectInputStream
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ObjectInputStreamWithClassLoader(final InputStream in) throws IOException
   {
      super(in);
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   @Override
   protected Class resolveClass(final ObjectStreamClass desc) throws IOException, ClassNotFoundException
   {
      String name = desc.getName();
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      try
      {
         Class clazz = loader.loadClass(name);
         // sanity check only.. if a classLoader can't find a clazz, it will throw an exception
         if (clazz == null)
         {
            return super.resolveClass(desc);
         }
         else
         {
            return clazz;
         }
      }
      catch (ClassNotFoundException e)
      {
         return super.resolveClass(desc);
      }
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
