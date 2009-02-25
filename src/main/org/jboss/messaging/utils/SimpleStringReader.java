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

package org.jboss.messaging.utils;

import java.io.IOException;
import java.io.Reader;

/**
 * A SimpleStringReader
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 31 oct. 2008 14:41:18
 *
 *
 */
public class SimpleStringReader extends Reader
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final SimpleString simpleString;

   private int next = 0;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SimpleStringReader(final SimpleString simpleString)
   {
      this.simpleString = simpleString;
   }

   // Public --------------------------------------------------------

   // Reader overrides ----------------------------------------------

   @Override
   public int read(char[] cbuf, int off, int len) throws IOException
   {
      synchronized (simpleString)
      {
         if ((off < 0) || (off > cbuf.length) || (len < 0) || ((off + len) > cbuf.length) || ((off + len) < 0))
         {
            throw new IndexOutOfBoundsException();
         }
         else if (len == 0)
         {
            return 0;
         }
         int length = simpleString.length();
         if (next >= length)
         {
            return -1;
         }
         int n = Math.min(length - next, len);
         simpleString.getChars(next, next + n, cbuf, off);
         next += n;
         return n;
      }
   }

   @Override
   public void close() throws IOException
   {
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
