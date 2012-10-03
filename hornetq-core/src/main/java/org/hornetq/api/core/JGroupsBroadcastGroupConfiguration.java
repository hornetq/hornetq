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

package org.hornetq.api.core;

import java.net.URL;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;

/**
 */
public class JGroupsBroadcastGroupConfiguration implements BroadcastEndpointFactoryConfiguration
{
   private static final long serialVersionUID = 8952238567248461285L;

   private final BroadcastEndpointFactory factory;

   public JGroupsBroadcastGroupConfiguration(final String jgroupsFile, final String channelName)
   {
      factory = new BroadcastEndpointFactory() {
         @Override
         public BroadcastEndpoint createBroadcastEndpoint() throws Exception {
            return new JGroupsBroadcastEndpoint(jgroupsFile, channelName);
         }
      };
   }

   public JGroupsBroadcastGroupConfiguration(final JChannel channel, final String channelName)
   {
      factory = new BroadcastEndpointFactory() {
         @Override
         public BroadcastEndpoint createBroadcastEndpoint() throws Exception {
            return new JGroupsBroadcastEndpoint(channel, channelName);
         }
      };
   }

   @Override
   public BroadcastEndpointFactory createBroadcastEndpointFactory()
   {
       return factory;
   }

   /**
    * This class is the implementation of HornetQ members discovery that will use JGroups.
    * @author Howard Gao
    */
   private static class JGroupsBroadcastEndpoint implements BroadcastEndpoint
   {
      private BlockingQueue<byte[]> dequeue = new LinkedBlockingDeque<byte[]>();

      private boolean clientOpened;

      private boolean broadcastOpened;

      private final String channelName;

      private final JChannel channel;

      public JGroupsBroadcastEndpoint(final String fileName, final String channelName) throws Exception
      {
         URL configURL = Thread.currentThread().getContextClassLoader().getResource(fileName);
         if (configURL == null)
         {
            throw new RuntimeException("couldn't find JGroups configuration " + fileName);
         }
         this.channel = new JChannel(configURL);
         this.channelName = channelName;
      }

      public JGroupsBroadcastEndpoint(final JChannel channel, final String channelName)
      {
         this.channel = channel;
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
            return dequeue.take();
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
            return dequeue.poll(time, unit);
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


      protected void internalOpen() throws Exception
      {
         if (channel.isConnected()) return;
         channel.connect(this.channelName);
      }

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
         channel.close();
      }

      private class JGroupsReceiver extends ReceiverAdapter
      {
         public void receive(org.jgroups.Message msg)
         {
            dequeue.add(msg.getBuffer());
         }
      }
   }
}
