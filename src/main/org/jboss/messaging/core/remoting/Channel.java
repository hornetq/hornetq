/*
 * JBoss, Home of Professional Open Source Copyright 2005, JBoss Inc., and individual contributors as indicated by the
 * @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is free
 * software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package org.jboss.messaging.core.remoting;

import java.util.Queue;

import org.jboss.messaging.core.exception.MessagingException;

/**
 * A Channel A Channel *does not* support concurrent access by more than one thread!
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface Channel
{
   long getID();

   void send(Packet packet);

   Packet sendBlocking(Packet packet) throws MessagingException;

   DelayedResult replicatePacket(Packet packet);
   
   void replicateComplete();

   void setHandler(ChannelHandler handler);

   void close();

   void fail();

   Channel getReplicatingChannel();

   void transferConnection(RemotingConnection newConnection);
   
   void replayCommands(int lastReceivedCommandID);

   int getLastReceivedCommandID();

   void lock();

   void unlock();

   void interruptBlocking();
   
   //debug only
   Queue<Command> getSentCommands();
   
   Queue<Command> getReceivedCommands();
   
   // For debug only
   static class Command
   {
      public final int commandID;

      public final Packet packet;

      public Command(final int commandID, final Packet packet)
      {
         this.commandID = commandID;

         this.packet = packet;
      }
   }
}
