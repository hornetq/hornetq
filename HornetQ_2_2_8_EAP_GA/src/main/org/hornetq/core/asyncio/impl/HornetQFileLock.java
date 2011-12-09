/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.asyncio.impl;

import java.io.IOException;
import java.nio.channels.FileLock;

/**
 * A HornetQFileLock
 *
 * @author clebertsuconic
 *
 *
 */
public class HornetQFileLock extends FileLock
{

   private final int handle;
   
   /**
    * @param channel
    * @param position
    * @param size
    * @param shared
    */
   protected HornetQFileLock(final int handle)
   {
      super(null, 0, 0, false);
      this.handle = handle;
   }

   @Override
   public boolean isValid()
   {
      return true;
   }

   /* (non-Javadoc)
    * @see java.nio.channels.FileLock#release()
    */
   @Override
   public void release() throws IOException
   {
      AsynchronousFileImpl.closeFile(handle);
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
