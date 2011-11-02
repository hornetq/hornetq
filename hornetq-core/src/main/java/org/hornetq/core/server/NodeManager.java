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

package org.hornetq.core.server;

import org.hornetq.api.core.SimpleString;
import org.hornetq.utils.UUID;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *         Date: Oct 13, 2010
 *         Time: 2:38:40 PM
 */
public abstract class NodeManager implements HornetQComponent
{
   public abstract void awaitLiveNode() throws Exception;

   public abstract void startBackup() throws Exception;

   public abstract void startLiveNode() throws Exception;

   public abstract void pauseLiveServer() throws Exception;

   public abstract void crashLiveServer() throws Exception;

   public abstract void stopBackup() throws Exception;

   public abstract void releaseBackup() throws Exception;

   private boolean isStarted = false;

   protected volatile SimpleString nodeID;

   protected volatile UUID uuid;

   public void start() throws Exception
   {
      isStarted = true;
   }

   public void stop() throws Exception
   {
      isStarted = false;
   }

   public boolean isStarted()
   {
      return isStarted;
   }


   public SimpleString getNodeId()
   {
      return nodeID;
   }

   public UUID getUUID()
   {
      return uuid;
   }

   public void setNodeID(String nodeID)
   {
      this.nodeID = new SimpleString(nodeID);
   }

   public abstract boolean isAwaitingFailback() throws Exception;

   public abstract boolean isBackupLive() throws Exception;

   public abstract void interrupt();
}
