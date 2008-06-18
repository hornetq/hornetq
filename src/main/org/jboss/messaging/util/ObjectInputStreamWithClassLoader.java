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

package org.jboss.messaging.util;

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

   public ObjectInputStreamWithClassLoader(InputStream in)
      throws IOException
   {
      super(in);
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected Class resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException
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
