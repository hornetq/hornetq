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

package org.hornetq.core.filter.impl;

import java.io.IOException;
import java.io.Reader;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;


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
   
   private static final Logger log = Logger.getLogger(SimpleStringReader.class);

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
   public int read(final char[] cbuf, final int off, final int len) throws IOException
   {
      synchronized (simpleString)
      {
         if (off < 0 || off > cbuf.length || len < 0 || off + len > cbuf.length || off + len < 0)
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
