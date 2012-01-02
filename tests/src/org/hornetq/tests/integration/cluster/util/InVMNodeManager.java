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

package org.hornetq.tests.integration.cluster.util;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.server.NodeManager;
import org.hornetq.utils.UUIDGenerator;

import java.util.concurrent.Semaphore;

import static org.hornetq.tests.integration.cluster.util.InVMNodeManager.State.*;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *         Date: Oct 13, 2010
 *         Time: 3:55:47 PM
 */
public class InVMNodeManager extends NodeManager
{

   private Semaphore liveLock;

   private Semaphore backupLock;

   public enum State {LIVE, PAUSED, FAILING_BACK, NOT_STARTED}

   public State state = NOT_STARTED;

   public long failoverPause = 0l;

   public InVMNodeManager()
   {
      liveLock = new Semaphore(1);
      backupLock = new Semaphore(1);
      uuid = UUIDGenerator.getInstance().generateUUID();
      nodeID = new SimpleString(uuid.toString());
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
      if(failoverPause > 0l)
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
   public void stopBackup() throws Exception
   {
      backupLock.release();
   }

   @Override
   public void releaseBackup()
   {
      releaseBackupNode();
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
      //To change body of implemented methods use File | Settings | File Templates.
   }

   private void releaseBackupNode()
   {
      if(backupLock != null)
      {
         backupLock.release();
      }
   }
}
