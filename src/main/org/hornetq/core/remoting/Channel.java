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
package org.hornetq.core.remoting;

import java.util.concurrent.locks.Lock;

import org.hornetq.core.exception.HornetQException;

/**
 * A Channel A Channel *does not* support concurrent access by more than one thread!
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface Channel
{
   long getID();

   void send(Packet packet);
   
   void sendAndFlush(Packet packet);
   
   Packet sendBlocking(Packet packet) throws HornetQException;

   void setHandler(ChannelHandler handler);
   
   ChannelHandler getHandler();

   void close();

   void transferConnection(RemotingConnection newConnection);
   
   void replayCommands(int lastReceivedCommandID, final long newID);

   int getLastReceivedCommandID();

   void lock();

   void unlock();

   void returnBlocking();
   
   Lock getLock();
   
   RemotingConnection getConnection();
   
   void confirm(Packet packet);
   
   void setCommandConfirmationHandler(CommandConfirmationHandler handler);
   
   void flushConfirmations();  
   
   void handlePacket(Packet packet);
}
