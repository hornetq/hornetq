/*
 * Copyright 2012 Red Hat, Inc.
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

package org.hornetq.api.core;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;


/**
 * This class is the implementation of HornetQ members discovery that will use JGroups.
 * @author Howard Gao
 */
public abstract class AbstractJGroupsBroadcastEndpoint implements BroadcastEndpoint
{
   private BlockingQueue<byte[]> dequeue = new LinkedBlockingDeque<byte[]>();

   private boolean clientOpened;

   private boolean broadcastOpened;

   protected final String channelName;

   protected JChannel channel;

   public AbstractJGroupsBroadcastEndpoint(final JChannel channel, final String channelName)
   {
      this.channelName = channelName;
      this.channel = channel;
   }

   public AbstractJGroupsBroadcastEndpoint(final String channelName)
   {
      this.channelName = channelName;
   }

   public void broadcast(byte[] data) throws Exception
   {
      if (broadcastOpened)
      {
         Message msg = new Message();
         msg.setBuffer(data);
         channel.send(msg);
      }
   }

   public byte[] receiveBroadcast() throws Exception
   {
      if (clientOpened)
      {
         byte[] msg = dequeue.take();
         return msg;
      }
      else
      {
         return null;
      }
   }

   public byte[] receiveBroadcast(long time, TimeUnit unit) throws Exception
   {
      if (clientOpened)
      {
         byte[] msg = dequeue.poll(time, unit);
         return msg;
      }
      else
      {
         return null;
      }
   }

   public synchronized void openClient() throws Exception
   {
      if (clientOpened)
      {
         return;
      }
      internalOpen();
      channel.setReceiver(new JGroupsReceiver());
      clientOpened = true;
   }

   public synchronized void openBroadcaster() throws Exception
   {
      if (broadcastOpened) return;
      internalOpen();
      broadcastOpened = true;
   }

   /**
    * There's no difference between the broadcaster and client on the JGropus implementation,
    * for that reason we can have a single internal method to open it
    * @throws Exception
    */
   protected abstract void internalOpen() throws Exception;

   public synchronized void close(boolean isBroadcast) throws Exception
   {
      if (isBroadcast)
      {
         broadcastOpened = false;
      }
      else
      {
         channel.setReceiver(null);
         clientOpened = false;
      }
   }

   private class JGroupsReceiver extends ReceiverAdapter
   {
      public void receive(org.jgroups.Message msg)
      {
         dequeue.add(msg.getBuffer());
      }
   }
}
