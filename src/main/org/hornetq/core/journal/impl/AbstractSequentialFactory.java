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
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.logging.Logger;

/**
 * 
 * An abstract SequentialFileFactory containing basic functionality for both AIO and NIO SequentialFactories
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public abstract class AbstractSequentialFactory implements SequentialFileFactory
{
   private static final Logger log = Logger.getLogger(AbstractSequentialFactory.class);

   protected final String journalDir;

   public AbstractSequentialFactory(final String journalDir)
   {
      this.journalDir = journalDir;
   }

   
   public void stop()
   {
   }
   
   public void start()
   {
   }
   
   public void activateBuffer(SequentialFile file)
   {
   }
   
   public void releaseBuffer(ByteBuffer buffer)
   {
   }
   
   public void deactivateBuffer()
   {
   }
   
   public void flush()
   {
   }

   /** 
    * Create the directory if it doesn't exist yet
    */
   public void createDirs() throws Exception
   {
      File file = new File(journalDir);
      boolean ok = file.mkdirs();
      if (!ok)
      {
         throw new IOException("Failed to create directory " + journalDir);
      }
   }

   public List<String> listFiles(final String extension) throws Exception
   {
      File dir = new File(journalDir);

      FilenameFilter fnf = new FilenameFilter()
      {
         public boolean accept(final File file, final String name)
         {
            return name.endsWith("." + extension);
         }
      };

      String[] fileNames = dir.list(fnf);

      if (fileNames == null)
      {
         throw new IOException("Failed to list: " + journalDir);
      }

      return Arrays.asList(fileNames);
   }

}
