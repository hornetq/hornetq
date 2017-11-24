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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;

import org.hornetq.api.core.HornetQIllegalStateException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.NodeManager;
import org.hornetq.utils.UUID;
import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *         Date: Oct 13, 2010
 *         Time: 2:44:02 PM
 */
public class FileLockNodeManager extends NodeManager
{
   private static final Logger logger = Logger.getLogger(FileLockNodeManager.class);

   private static final long STATE_LOCK_POS = 0;

   private static final long LIVE_LOCK_POS = 1;

   private static final long BACKUP_LOCK_POS = 2;

   private static final long LOCK_LENGTH = 1;

   private static final byte LIVE = 'L';

   private static final byte FAILINGBACK = 'F';

   private static final byte PAUSED = 'P';

   private static final byte NOT_STARTED = 'N';

   private FileLock liveLock;

   private FileLock backupLock;

   protected boolean interrupted = false;

   public FileLockNodeManager(final String directory, boolean replicatedBackup)
   {
      super(replicatedBackup, directory);
   }

   @Override
   public synchronized void start() throws Exception
   {
      if (isStarted())
      {
         return;
      }
      if (!replicatedBackup)
      {
         setUpServerLockFile();
      }

      super.start();
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
   public final void releaseBackup() throws Exception
   {
      if (backupLock != null)
      {
         backupLock.release();
         backupLock = null;
      }
   }

   @Override
   public void awaitLiveNode() throws Exception
   {
      logger.debug("awaitng live node...");
      do
      {
         byte state = getState();
         while (state == FileLockNodeManager.NOT_STARTED || state == FIRST_TIME_START)
         {
            HornetQServerLogger.LOGGER.debug("awaiting live node startup state='" + state + "'");
            Thread.sleep(2000);
            state = getState();
         }

         liveLock = lock(FileLockNodeManager.LIVE_LOCK_POS);
         state = getState();
         if (state == FileLockNodeManager.PAUSED)
         {
            liveLock.release();
            HornetQServerLogger.LOGGER.debug("awaiting live node restarting");
            Thread.sleep(2000);
         }
         else if (state == FileLockNodeManager.FAILINGBACK)
         {
            liveLock.release();
            HornetQServerLogger.LOGGER.debug("awaiting live node failing back");
            Thread.sleep(2000);
         }
         else if (state == FileLockNodeManager.LIVE)
         {
            HornetQServerLogger.LOGGER.debug("acquired live node lock state = " + (char)state);
            break;
         }
      }
      while (true);
   }

   @Override
   public void startBackup() throws Exception
   {
      assert !replicatedBackup; // should not be called if this is a replicating backup
      HornetQServerLogger.LOGGER.waitingToBecomeBackup();

      backupLock = lock(FileLockNodeManager.BACKUP_LOCK_POS);
      HornetQServerLogger.LOGGER.gotBackupLock();
      if (getUUID() == null)
         readNodeId();
   }

   @Override
   public void startLiveNode() throws Exception
   {
      setFailingBack();

      HornetQServerLogger.LOGGER.waitingToObtainLiveLock();

      liveLock = lock(FileLockNodeManager.LIVE_LOCK_POS);

      HornetQServerLogger.LOGGER.obtainedLiveLock();

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

   private void setLive() throws Exception
   {
      writeFileLockStatus(FileLockNodeManager.LIVE);
   }

   private void setFailingBack() throws Exception
   {
      writeFileLockStatus(FAILINGBACK);
   }

   private void setPaused() throws Exception
   {
      writeFileLockStatus(PAUSED);
   }

   /**
    * @param status
    * @throws IOException
    */
   private void writeFileLockStatus(byte status) throws Exception
   {
      if (replicatedBackup && channel == null)
         return;
      logger.debug("wrting status: " + status);
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      bb.put(status);
      bb.position(0);

      if (!channel.isOpen())
      {
         setUpServerLockFile();
      }

      FileLock lock = null;

      try
      {
         lock = lock(STATE_LOCK_POS);
         channel.write(bb, 0);
         channel.force(true);
      }
      finally
      {
         if (lock != null)
         {
            lock.release();
         }
      }

   }

   private byte getState() throws Exception
   {
      byte result;
      logger.debug("getting state...");
      ByteBuffer bb = ByteBuffer.allocateDirect(1);
      int read;
      FileLock lock = null;
      try
      {
         lock = lock(STATE_LOCK_POS);
         read = channel.read(bb, 0);
         if (read <= 0)
         {
            result =  FileLockNodeManager.NOT_STARTED;
         }
         else
         {
            result = bb.get(0);
         }
      }
      finally
      {
         if (lock != null)
         {
            lock.release();
         }
      }

      logger.debug("state: " + result);

      return result;
   }

   @Override
   public final SimpleString readNodeId() throws HornetQIllegalStateException, IOException
   {
      ByteBuffer id = ByteBuffer.allocateDirect(16);
      int read = channel.read(id, 3);
      if (read != 16)
      {
         throw new HornetQIllegalStateException("live server did not write id to file");
      }
      byte[] bytes = new byte[16];
      id.position(0);
      id.get(bytes);
      setUUID(new UUID(UUID.TYPE_TIME_BASED, bytes));
      return getNodeId();
   }

   protected FileLock tryLock(final long lockPos) throws Exception
   {
      try
      {
         logger.debug("trying to lock position: " + lockPos);
         FileLock lock = channel.tryLock(lockPos, LOCK_LENGTH, false);
         if (lock != null)
         {
            logger.debug("locked postion: " + lockPos);
         }
         else
         {
            logger.debug("failed to lock postion: " + lockPos);
         }

         return lock;
      }
      catch (java.nio.channels.OverlappingFileLockException ex)
      {
         logger.trace(ex);
         return null;
      }


   }

   protected FileLock lock(final long lockPosition) throws Exception
   {
      long start = System.currentTimeMillis();

      while (!interrupted)
      {
         FileLock lock = tryLock(lockPosition);

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
         lock = tryLock(lockPosition);
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
