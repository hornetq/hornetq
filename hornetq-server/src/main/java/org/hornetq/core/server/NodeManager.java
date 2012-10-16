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

import java.io.IOException;

import org.hornetq.api.core.HornetQIllegalStateException;
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

   private final Object nodeIDGuard = new Object();
   private SimpleString nodeID;
   private UUID uuid;
   private String nodeGroupName;

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
      synchronized (nodeIDGuard)
      {
         return nodeID;
      }
   }

   public abstract SimpleString readNodeId() throws HornetQIllegalStateException, IOException;

   public UUID getUUID()
   {
      synchronized (nodeIDGuard)
      {
         return uuid;
      }
   }

   /**
    * Sets the nodeID.
    * <p>
    * Only used by replicating backups.
    * @param nodeID
    */
   public void setNodeID(String nodeID)
   {
      synchronized (nodeIDGuard)
      {
         this.nodeID = new SimpleString(nodeID);
         this.uuid = new UUID(UUID.TYPE_TIME_BASED, UUID.stringToBytes(nodeID));
      }
   }

   /**
    * @param generateUUID
    */
   protected void setUUID(UUID generateUUID)
   {
      synchronized (nodeIDGuard)
      {
         uuid = generateUUID;
         nodeID = new SimpleString(uuid.toString());
      }
   }

   public void setNodeGroupName(String nodeGroupName)
   {
      this.nodeGroupName = nodeGroupName;
   }

   public String getNodeGroupName()
   {
      return nodeGroupName;
   }

   public abstract boolean isAwaitingFailback() throws Exception;

   public abstract boolean isBackupLive() throws Exception;

   public abstract void interrupt();

}
