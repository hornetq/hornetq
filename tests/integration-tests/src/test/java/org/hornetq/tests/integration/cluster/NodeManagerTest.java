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

package org.hornetq.tests.integration.cluster;

import org.junit.Test;

import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.tests.util.ServiceTestBase;

import java.util.ArrayList;
import java.util.List;

import static org.hornetq.tests.integration.cluster.NodeManagerAction.*;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *         Date: Oct 16, 2010
 *         Time: 9:22:32 AM
 */
public class NodeManagerTest extends ServiceTestBase
{
   @Test
   public void testLive() throws Exception
   {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(live1);
   }
   @Test
   public void testSimpleLiveAndBackup() throws Exception
   {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, PAUSE_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(live1, backup1);
   }

   @Test
    public void testSimpleBackupAndLive() throws Exception
   {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, PAUSE_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(backup1, live1);
   }

   @Test
   public void testSimpleLiveAnd2Backups() throws Exception
   {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup2 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(live1, backup1, backup2);
   }


   @Test
   public void testSimple2BackupsAndLive() throws Exception
   {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup2 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(backup1, backup2, live1);
   }

   @Test
   public void testSimpleLiveAnd2BackupsPaused() throws Exception
   {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, PAUSE_LIVE, START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup2 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, PAUSE_LIVE, START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(live1, backup1, backup2);
   }

   @Test
   public void testSimple2BackupsPausedAndLive() throws Exception
   {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, PAUSE_LIVE, START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup2 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, PAUSE_LIVE, START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(backup1, backup2, live1);
   }

   @Test
   public void testBackupsOnly() throws Exception
   {
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup2 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup3 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup4 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup5 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup6 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup7 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup8 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup9 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup10 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup11 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(backup1,backup2,backup3,backup4,backup5,backup6,backup7,backup8,backup9,backup10,backup11);
   }

   @Test
   public void testLiveAndBackupLiveForcesFailback() throws Exception
   {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, AWAIT_LIVE, HAS_LIVE, PAUSE_LIVE);
      performWork(live1, backup1);
   }

   @Test
   public void testLiveAnd2BackupsLiveForcesFailback() throws Exception
   {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE);
      NodeManagerAction backup2 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE);
      performWork(live1, backup1, backup2);
   }

   public void performWork(NodeManagerAction... actions) throws Exception
   {
      NodeManager nodeManager = new InVMNodeManager(false);
      List<NodeRunner> nodeRunners = new ArrayList<NodeRunner>();
      Thread[] threads = new Thread[actions.length];
      for (NodeManagerAction action : actions)
      {
         NodeRunner nodeRunner = new NodeRunner(nodeManager, action);
         nodeRunners.add(nodeRunner);
      }
      for (int i = 0, nodeRunnersSize = nodeRunners.size(); i < nodeRunnersSize; i++)
      {
         NodeRunner nodeRunner = nodeRunners.get(i);
         threads[i] = new Thread(nodeRunner);
         threads[i].start();
      }

      for (Thread thread : threads)
      {
         try
         {
            thread.join(5000);
         }
         catch (InterruptedException e)
         {
            //
         }
         if(thread.isAlive())
         {
            thread.interrupt();
            fail("thread still running");
         }
      }

      for (NodeRunner nodeRunner : nodeRunners)
      {
         if(nodeRunner.e != null)
         {
            nodeRunner.e.printStackTrace();
            fail(nodeRunner.e.getMessage());
         }
      }
   }

   static class NodeRunner implements Runnable
   {
      private NodeManagerAction action;
      private NodeManager manager;
      Throwable e;
      public NodeRunner(NodeManager nodeManager, NodeManagerAction action)
      {
         this.manager = nodeManager;
         this.action = action;
      }

      public void run()
      {
         try
         {
            action.performWork(manager);
         }
         catch (Throwable e)
         {
            this.e = e;
         }
      }
   }


}
