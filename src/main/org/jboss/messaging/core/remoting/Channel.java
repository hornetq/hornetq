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

import java.util.concurrent.locks.Lock;

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

   void replicatePacket(Packet packet, long replicatedChannelID, Runnable action);
   
   void setHandler(ChannelHandler handler);

   void close();

   void transferConnection(RemotingConnection newConnection, final long newID, final Channel replicatingChannel);
   
   void replayCommands(int lastReceivedCommandID, final long newID);

   int getLastReceivedCommandID();

   void lock();

   void unlock();

   void returnBlocking();
   
   Lock getLock();
   
   RemotingConnection getConnection();
   
   void executeOutstandingDelayedResults();
   
   void confirm(Packet packet);
   
   void setCommandConfirmationHandler(CommandConfirmationHandler handler);
   
   void flushConfirmations();      
}
