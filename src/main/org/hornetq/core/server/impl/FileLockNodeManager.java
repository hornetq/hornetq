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

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.NodeManager;
import org.hornetq.utils.UUID;
import org.hornetq.utils.UUIDGenerator;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *         Date: Oct 13, 2010
 *         Time: 2:44:02 PM
 */
public class FileLockNodeManager extends NodeManager
{
   private static final Logger log = Logger.getLogger(FileLockNodeManager.class);

   private static final String SERVER_LOCK_NAME = "server.lock";

   private static final String ACCESS_MODE = "rw";

   private static  final int LIVE_LOCK_POS = 1;

   private static  final int BACKUP_LOCK_POS = 2;

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


   public FileLockNodeManager(final String directory)
   {
      this.directory = directory;
   }

   public void start() throws Exception
   {
      if(isStarted())
      {
         return;
      }
      File file = new File(directory, SERVER_LOCK_NAME);

      boolean fileCreated = false;

      if (!file.exists())
      {
         fileCreated = file.createNewFile();
         if(!fileCreated)
         {
            throw new IllegalStateException("Unable to create server lock file");
         }
      }

      RandomAccessFile raFile = new RandomAccessFile(file, ACCESS_MODE);

      channel = raFile.getChannel();

      if (fileCreated)
      {
         ByteBuffer id = ByteBuffer.allocateDirect(3);
         byte[] bytes = new byte[3];
         bytes[0] = FIRST_TIME_START;
         bytes[1] = FIRST_TIME_START;
         bytes[2] = FIRST_TIME_START;
         id.put(bytes, 0, 3);
         id.position(0);
         channel.write(id, 0);
         channel.force(true);
      }

      createNodeId();

      super.start();
   }

   public void stop() throws Exception
   {
      channel.close();

      super.stop();
   }

   @Override
   public boolean isAwaitingFailback() throws Exception
   {
      return getState() == FAILINGBACK;
   }

   public boolean isBackupLive() throws Exception
   {
      FileLock liveAttemptLock;
      liveAttemptLock = channel.tryLock(LIVE_LOCK_POS, LOCK_LENGTH, false);
      if(liveAttemptLock == null)
      {
         return true;
      }
      else
      {
         liveAttemptLock.release();
         return false;
      }
   }
   @Override
   public void releaseBackup() throws Exception
   {
      releaseBackupLock();
   }


   public void awaitLiveNode() throws Exception
   {
      do
      {
         byte state = getState();
         while (state == NOT_STARTED || state == FIRST_TIME_START)
         {
            log.debug("awaiting live node startup state='" + state + "'");
            Thread.sleep(2000);
            state = getState();
         }

         liveLock = lock(LIVE_LOCK_POS, 1);
         state = getState();
         if (state == PAUSED)
         {
            liveLock.release();
            log.debug("awaiting live node restarting");
            Thread.sleep(2000);
         }
         else if (state == FAILINGBACK)
         {
            liveLock.release();
            log.debug("awaiting live node failing back");
            Thread.sleep(2000);
         }
         else if (state == LIVE)
         {
            log.debug("acquired live node lock state = " + (char)state);
            break;
         }
      }
      while (true);
   }

   public void startBackup() throws Exception
   {

      log.info("Waiting to become backup node");

      backupLock = lock(BACKUP_LOCK_POS, LOCK_LENGTH);
      log.info("** got backup lock");

      readNodeId();
   }

   public void startLiveNode() throws Exception
   {
      setFailingBack();

      log.info("Waiting to obtain live lock");

      liveLock = lock(LIVE_LOCK_POS, LOCK_LENGTH);

      log.info("Live Server Obtained live lock");

      setLive();
   }

   public void pauseLiveServer() throws Exception
   {
      setPaused();
      liveLock.release();
   }

   public void crashLiveServer() throws Exception
   {
      liveLock.release();
   }

   public void stopBackup() throws Exception
   {
      backupLock.release();
   }

   private void setLive() throws Exception
   {
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      bb.put(LIVE);
      bb.position(0);
      channel.write(bb, 0);
      channel.force(true);
   }

   private void setFailingBack() throws Exception
   {
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      bb.put(FAILINGBACK);
      bb.position(0);
      channel.write(bb, 0);
      channel.force(true);
   }

   private void setPaused() throws Exception
   {
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      bb.put(PAUSED);
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
         return NOT_STARTED;
      }
      else
         return bb.get(0);
   }

   private void releaseBackupLock() throws Exception
   {
      if (backupLock != null)
      {
         backupLock.release();
      }
   }

   private void createNodeId() throws Exception
   {
      ByteBuffer id = ByteBuffer.allocateDirect(16);
      int read = channel.read(id, 3);
      if(read != 16)
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
      if(read != 16)
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

   private FileLock lock(int liveLockPos, int i) throws IOException
   {
      try
      {
         return channel.lock(liveLockPos, i, false);
      }
      catch (IOException e)
      {
         //todo this is here because sometimes channel.lock throws a resource deadlock exception but trylock works, need to investigate further and review
         FileLock lock;
         do
         {
            lock = channel.tryLock(liveLockPos, i, false);
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
            if (Thread.currentThread().isInterrupted())
            {
               throw new IOException(new InterruptedException());
            }
         }
         while(lock == null);
         return lock;
      }
   }
}

