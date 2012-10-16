/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.core.server.cluster;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.spi.core.protocol.RemotingConnection;

/**
 * A Core Bridge
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 15 Nov 2008 09:42:31
 *
 *
 */
public interface Bridge extends Consumer, HornetQComponent
{
   SimpleString getName();

   Queue getQueue();

   SimpleString getForwardingAddress();

   Transformer getTransformer();

   boolean isUseDuplicateDetection();

   void activate();
   
   void flushExecutor();
   
   void setNotificationService(NotificationService notificationService);

   RemotingConnection getForwardingConnection();

   void pause() throws Exception;

   void resume() throws Exception;

   /**
    * To be called when the server sent a disconnect to the client.
    * Basically this is for cluster bridges being disconnected
    */
   void disconnect();

   boolean isConnected();
}
