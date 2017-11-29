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

package org.hornetq.core.server.impl;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;

import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;

/**
 * This is using the HornetQ Libaio Native to perform calls to flock on a Linux system. At the
 * current version of RHEL there's a bug on GFS2 and because of that fctl is not functional what
 * will cause issues on Failover over Shared Storage.
 * <p/>
 * This will provide an alternative to perform locks through our native module until fctl is fixed
 * on Linux.
 * <p/>
 * https://bugzilla.redhat.com/show_bug.cgi?id=678585
 *
 * @author clebertsuconic
 */
public final class AIOFileLockNodeManager extends FileLockNodeManager
{

   /**
    * @param directory
    * @param replicatingBackup
    */
   public AIOFileLockNodeManager(final String directory, boolean replicatingBackup)
   {
      super(directory, replicatingBackup);
   }

   @Override
   protected FileLock tryLock(final long lockPos) throws Exception
   {
      File file = newFileForRegionLock(lockPos);

      int handle = AsynchronousFileImpl.openFile(file.getAbsolutePath());

      if (handle < 0)
      {
         throw new IOException("couldn't open file " + file.getAbsolutePath());
      }

      FileLock lock = AsynchronousFileImpl.lock(handle);

      if (lock == null)
      {
         AsynchronousFileImpl.closeFile(handle);
      }

      return lock;

   }

   @Override
   protected FileLock lock(final long liveLockPos) throws IOException
   {
      File file = newFileForRegionLock(liveLockPos);

      while (!interrupted)
      {
         int handle = AsynchronousFileImpl.openFile(file.getAbsolutePath());

         if (handle < 0)
         {
            throw new IOException("couldn't open file " + file.getAbsolutePath());
         }

         FileLock lockFile = AsynchronousFileImpl.lock(handle);
         if (lockFile != null)
         {
            return lockFile;
         }
         else
         {
            AsynchronousFileImpl.closeFile(handle);
            try
            {
               Thread.sleep(500);
            }
            catch (InterruptedException e)
            {
               return null;
            }
         }
      }

      return null;
   }

   /**
    * @param liveLockPos
    * @return
    */
   protected File newFileForRegionLock(final long liveLockPos)
   {
      File file = newFile("server." + liveLockPos + ".lock");
      return file;
   }
}
