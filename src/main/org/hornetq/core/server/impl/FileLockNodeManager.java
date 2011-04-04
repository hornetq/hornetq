/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package org.hornetq.core.server.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.NodeManager;
import org.hornetq.utils.UUID;
import org.hornetq.utils.UUIDGenerator;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *         Date: Oct 13, 2010
 *         Time: 2:44:02 PM
 */
public class FileLockNodeManager extends NodeManager
{
   private final Logger log = Logger.getLogger(this.getClass());

   protected static final String SERVER_LOCK_NAME = "server.lock";

   private static final String ACCESS_MODE = "rw";

   private static final int LIVE_LOCK_POS = 1;

   private static final int BACKUP_LOCK_POS = 2;

   private static final int LOCK_LENGTH = 1;

   private static final byte LIVE = 'L';

   private static final byte FAILINGBACK = 'F';

   private static final byte PAUSED = 'P';

   private static final byte NOT_STARTED = 'N';

   private static final byte FIRST_TIME_START = '0';

   private FileChannel channel;

   private FileLock liveLock;

   private FileLock backupLock;

   private final String directory;

   protected boolean interrupted = false;

   public FileLockNodeManager(final String directory)
   {
      this.directory = directory;
   }

   @Override
   public void start() throws Exception
   {
      if (isStarted())
      {
         return;
      }
      File file = newFile(FileLockNodeManager.SERVER_LOCK_NAME);

      boolean fileCreated = false;

      if (!file.exists())
      {
         fileCreated = file.createNewFile();
         if (!fileCreated)
         {
            throw new IllegalStateException("Unable to create server lock file");
         }
      }

      RandomAccessFile raFile = new RandomAccessFile(file, FileLockNodeManager.ACCESS_MODE);

      channel = raFile.getChannel();

      if (fileCreated)
      {
         ByteBuffer id = ByteBuffer.allocateDirect(3);
         byte[] bytes = new byte[3];
         bytes[0] = FileLockNodeManager.FIRST_TIME_START;
         bytes[1] = FileLockNodeManager.FIRST_TIME_START;
         bytes[2] = FileLockNodeManager.FIRST_TIME_START;
         id.put(bytes, 0, 3);
         id.position(0);
         channel.write(id, 0);
         channel.force(true);
      }

      createNodeId();

      super.start();
   }

   @Override
   public void stop() throws Exception
   {
      channel.close();

      super.stop();
   }

   @Override
   public boolean isAwaitingFailback() throws Exception
   {
      return getState() == FileLockNodeManager.FAILINGBACK;
   }

   @Override
   public boolean isBackupLive() throws Exception
   {
      FileLock liveAttemptLock;
      liveAttemptLock = tryLock(FileLockNodeManager.LIVE_LOCK_POS);
      if (liveAttemptLock == null)
      {
         return true;
      }
      else
      {
         liveAttemptLock.release();
         return false;
      }
   }
   
   public boolean isLiveLocked()
   {
      return liveLock != null;
   }

   
   @Override
   public void interrupt()
   {
      interrupted = true;
   }

   @Override
   public void releaseBackup() throws Exception
   {
      releaseBackupLock();
   }

   @Override
   public void awaitLiveNode() throws Exception
   {
      do
      {
         byte state = getState();
         while (state == FileLockNodeManager.NOT_STARTED || state == FileLockNodeManager.FIRST_TIME_START)
         {
            log.debug("awaiting live node startup state='" + state + "'");
            Thread.sleep(2000);
            state = getState();
         }

         liveLock = lock(FileLockNodeManager.LIVE_LOCK_POS);
         state = getState();
         if (state == FileLockNodeManager.PAUSED)
         {
            liveLock.release();
            log.debug("awaiting live node restarting");
            Thread.sleep(2000);
         }
         else if (state == FileLockNodeManager.FAILINGBACK)
         {
            liveLock.release();
            log.debug("awaiting live node failing back");
            Thread.sleep(2000);
         }
         else if (state == FileLockNodeManager.LIVE)
         {
            log.debug("acquired live node lock state = " + (char)state);
            break;
         }
      }
      while (true);
   }

   @Override
   public void startBackup() throws Exception
   {

      log.info("Waiting to become backup node");

      backupLock = lock(FileLockNodeManager.BACKUP_LOCK_POS);
      log.info("** got backup lock");

      readNodeId();
   }

   @Override
   public void startLiveNode() throws Exception
   {
      setFailingBack();

      log.info("Waiting to obtain live lock");

      liveLock = lock(FileLockNodeManager.LIVE_LOCK_POS);

      log.info("Live Server Obtained live lock");

      setLive();
   }

   @Override
   public void pauseLiveServer() throws Exception
   {
      setPaused();
      if (liveLock != null)
      {
         liveLock.release();
      }
   }

   @Override
   public void crashLiveServer() throws Exception
   {
      if (liveLock != null)
      {
         liveLock.release();
         liveLock = null;
      }
   }

   @Override
   public void stopBackup() throws Exception
   {
      if (backupLock != null)
      {
         backupLock.release();
         backupLock = null;
      }
      
   }

   public String getDirectory()
   {
      return directory;
   }

   private void setLive() throws Exception
   {
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      bb.put(FileLockNodeManager.LIVE);
      bb.position(0);
      channel.write(bb, 0);
      channel.force(true);
   }

   private void setFailingBack() throws Exception
   {
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      bb.put(FileLockNodeManager.FAILINGBACK);
      bb.position(0);
      channel.write(bb, 0);
      channel.force(true);
   }

   private void setPaused() throws Exception
   {
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      bb.put(FileLockNodeManager.PAUSED);
      bb.position(0);
      channel.write(bb, 0);
      channel.force(true);
   }

   private byte getState() throws Exception
   {
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      int read;
      read = channel.read(bb, 0);
      if (read <= 0)
      {
         return FileLockNodeManager.NOT_STARTED;
      }
      else
      {
         return bb.get(0);
      }
   }

   private void releaseBackupLock() throws Exception
   {
      if (backupLock != null)
      {
         backupLock.release();
         backupLock = null;
      }
   }

   private void createNodeId() throws Exception
   {
      ByteBuffer id = ByteBuffer.allocateDirect(16);
      int read = channel.read(id, 3);
      if (read != 16)
      {
         uuid = UUIDGenerator.getInstance().generateUUID();
         nodeID = new SimpleString(uuid.toString());
         id.put(uuid.asBytes(), 0, 16);
         id.position(0);
         channel.write(id, 3);
         channel.force(true);
      }
      else
      {
         byte[] bytes = new byte[16];
         id.position(0);
         id.get(bytes);
         uuid = new UUID(UUID.TYPE_TIME_BASED, bytes);
         nodeID = new SimpleString(uuid.toString());
      }
   }

   private void readNodeId() throws Exception
   {
      ByteBuffer id = ByteBuffer.allocateDirect(16);
      int read = channel.read(id, 3);
      if (read != 16)
      {
         throw new IllegalStateException("live server did not write id to file");
      }
      else
      {
         byte[] bytes = new byte[16];
         id.position(0);
         id.get(bytes);
         uuid = new UUID(UUID.TYPE_TIME_BASED, bytes);
         nodeID = new SimpleString(uuid.toString());
      }
   }

   /**
    * @return
    */
   protected File newFile(final String fileName)
   {
      File file = new File(directory, fileName);
      return file;
   }

   protected FileLock tryLock(final int lockPos) throws Exception
   {
      return channel.tryLock(lockPos, LOCK_LENGTH, false);
   }


   protected FileLock lock(final int liveLockPos) throws IOException
   {
      while (!interrupted)
      {
         FileLock lock = null;
         try
         {
            lock = channel.tryLock(liveLockPos, 1, false);
         }
         catch (java.nio.channels.OverlappingFileLockException ex)
         {
            // This just means that another object on the same JVM is holding the lock
         }
         
         if (lock == null)
         {
            try
            {
               Thread.sleep(500);
            }
            catch (InterruptedException e)
            {
               return null;
            }
         }
         else
         {
            return lock;
         }
      }
         
      // todo this is here because sometimes channel.lock throws a resource deadlock exception but trylock works,
      // need to investigate further and review
      FileLock lock;
      do
      {
         lock = channel.tryLock(liveLockPos, 1, false);
         if (lock == null)
         {
            try
            {
               Thread.sleep(500);
            }
            catch (InterruptedException e1)
            {
               //
            }
         }
         if (interrupted)
         {
            interrupted = false;
            throw new IOException("Lock was interrupted");
         }
      }
      while (lock == null);
      return lock;
   }

}
