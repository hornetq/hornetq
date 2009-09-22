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

package org.hornetq.core.journal.impl;

import java.io.File;

import org.hornetq.core.journal.SequentialFile;

/**
 * A AbstractSequentialFile
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public abstract class AbstractSequentialFile implements SequentialFile
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private File file;

   private final String directory;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * @param file
    * @param directory
    */
   public AbstractSequentialFile(String directory, File file)
   {
      super();
      this.file = file;
      this.directory = directory;
   }

   // Public --------------------------------------------------------

   public final boolean exists()
   {
      return file.exists();
   }

   public final String getFileName()
   {
      return file.getName();
   }


   public final void delete() throws Exception
   {
      if (isOpen())
      {
         close();
      }

      file.delete();
   }


   public final void renameTo(final String newFileName) throws Exception
   {
      close();
      File newFile = new File(directory + "/" + newFileName);
      

      if (!file.equals(newFile))
      {
         file.renameTo(newFile);
         file = newFile;
      }
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected File getFile()
   {
      return file;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
