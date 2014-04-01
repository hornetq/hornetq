/*
 * Copyright 2005-2014 Red Hat, Inc.
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

import static org.hornetq.core.server.impl.InVMNodeManager.State.LIVE;
import static org.hornetq.core.server.impl.InVMNodeManager.State.FAILING_BACK;
import static org.hornetq.core.server.impl.InVMNodeManager.State.NOT_STARTED;
import static org.hornetq.core.server.impl.InVMNodeManager.State.PAUSED;

import java.io.IOException;
import java.util.concurrent.Semaphore;

import org.hornetq.api.core.HornetQIllegalStateException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.server.NodeManager;
import org.hornetq.utils.UUIDGenerator;

/**
 * NodeManager used to run multiple servers in the same VM.
 * <p/>
 * We use the {@link org.hornetq.core.server.impl.InVMNodeManager} instead of {@link org.hornetq.core.server.impl.FileLockNodeManager} when
 * multiple servers are run inside the same VM and File Locks can not be shared in the
 * same VM (it would cause a shared lock violation).
 *
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a> Date: Oct 13, 2010 Time: 3:55:47
 *         PM
 */
public final class InVMNodeManager extends NodeManager
{

   private final Semaphore liveLock;

   private final Semaphore backupLock;

   public enum State
   {
      LIVE, PAUSED, FAILING_BACK, NOT_STARTED
   }

   public State state = NOT_STARTED;

   public long failoverPause = 0L;

   public InVMNodeManager(boolean replicatedBackup)
   {
      this(replicatedBackup, null);
      if (replicatedBackup)
         throw new RuntimeException("if replicated-backup, we need its journal directory");
   }

   public InVMNodeManager(boolean replicatedBackup, String directory)
   {
      super(replicatedBackup, directory);
      liveLock = new Semaphore(1);
      backupLock = new Semaphore(1);
      setUUID(UUIDGenerator.getInstance().generateUUID());
   }

   @Override
   public void awaitLiveNode() throws Exception
   {
      do
      {
         while (state == NOT_STARTED)
         {
            Thread.sleep(2000);
         }

         liveLock.acquire();

         if (state == PAUSED)
         {
            liveLock.release();
            Thread.sleep(2000);
         }
         else if (state == FAILING_BACK)
         {
            liveLock.release();
            Thread.sleep(2000);
         }
         else if (state == LIVE)
         {
            break;
         }
      }
      while (true);
      if (failoverPause > 0L)
      {
         Thread.sleep(failoverPause);
      }
   }

   @Override
   public void startBackup() throws Exception
   {
      backupLock.acquire();
   }

   @Override
   public void startLiveNode() throws Exception
   {
      state = FAILING_BACK;
      liveLock.acquire();
      state = LIVE;
   }

   @Override
   public void pauseLiveServer() throws Exception
   {
      state = PAUSED;
      liveLock.release();
   }

   @Override
   public void crashLiveServer() throws Exception
   {
      //overkill as already set to live
      state = LIVE;
      liveLock.release();
   }

   @Override
   public boolean isAwaitingFailback() throws Exception
   {
      return state == FAILING_BACK;
   }

   @Override
   public boolean isBackupLive() throws Exception
   {
      return liveLock.availablePermits() == 0;
   }

   @Override
   public void interrupt()
   {
      //
   }

   @Override
   public void releaseBackup()
   {
      if (backupLock != null)
      {
         backupLock.release();
      }
   }

   @Override
   public SimpleString readNodeId() throws HornetQIllegalStateException, IOException
   {
      return getNodeId();
   }
}
