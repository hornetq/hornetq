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

import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.impl.FileLockNodeManager;

import java.nio.channels.FileLock;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *         Date: Oct 18, 2010
 *         Time: 10:09:12 AM
 */
public class NodeManagerAction
{
   public final static int START_LIVE = 0;
   public final static int START_BACKUP = 1;
   public final static int CRASH_LIVE = 2;
   public final static int PAUSE_LIVE = 3;
   public final static int STOP_BACKUP = 4;
   public final static int AWAIT_LIVE = 5;
   public final static int RELEASE_BACKUP = 6;

   public final static int HAS_LIVE = 10;
   public final static int HAS_BACKUP = 11;
   public final static int DOESNT_HAVE_LIVE = 12;
   public final static int DOESNT_HAVE_BACKUP = 13;


   private final int[] work;

   boolean hasLiveLock = false;
   boolean hasBackupLock = false;

   public NodeManagerAction(int... work)
   {
      this.work = work;
   }

   public void performWork(NodeManager nodeManager) throws Exception
   {
      for (int action : work)
      {
         switch (action)
         {
            case START_LIVE:
               nodeManager.startLiveNode();
               hasLiveLock = true;
               hasBackupLock = false;
               break;
            case START_BACKUP:
               nodeManager.startBackup();
               hasBackupLock = true;
               break;
            case CRASH_LIVE:
               nodeManager.crashLiveServer();
               hasLiveLock = false;
               break;
            case PAUSE_LIVE:
               nodeManager.pauseLiveServer();
               hasLiveLock = false;
               break;
            case STOP_BACKUP:
               nodeManager.stopBackup();
               hasBackupLock = false;
               break;
            case AWAIT_LIVE:
               nodeManager.awaitLiveNode();
               hasLiveLock = true;
               break;
            case RELEASE_BACKUP:
               nodeManager.releaseBackup();
               hasBackupLock = false;
            case HAS_LIVE:
               if (!hasLiveLock)
               {
                  throw new IllegalStateException("live lock not held");
               }
               break;
            case HAS_BACKUP:

               if (!hasBackupLock)
               {
                  throw new IllegalStateException("backup lock not held");
               }
               break;
            case DOESNT_HAVE_LIVE:
               if (hasLiveLock)
               {
                  throw new IllegalStateException("live lock held");
               }
               break;
            case DOESNT_HAVE_BACKUP:

               if (hasBackupLock)
               {
                  throw new IllegalStateException("backup lock held");
               }
               break;
         }
      }
   }

   public String[] getWork()
   {
      String[] strings = new String[work.length];
      for (int i = 0, stringsLength = strings.length; i < stringsLength; i++)
      {
         strings[i] = "" + work[i];
      }
      return strings;
   }

   public static void main(String[] args) throws Exception
   {
      int[] work1 = new int[args.length];
      for (int i = 0; i < args.length; i++)
      {
         work1[i] = Integer.parseInt(args[i]);

      }
      NodeManagerAction nodeManagerAction = new NodeManagerAction(work1);
      FileLockNodeManager nodeManager = new FileLockNodeManager(".");
      nodeManager.start();
      try
      {
         nodeManagerAction.performWork(nodeManager);
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(9);
      }
      System.out.println("work performed");
   }

}
