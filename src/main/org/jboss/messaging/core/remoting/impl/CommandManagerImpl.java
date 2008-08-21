/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.messaging.core.remoting.impl;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.CommandManager;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketsConfirmedMessage;

/**
 * A CommandManagerImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class CommandManagerImpl implements CommandManager, PacketHandler
{
   private static final Logger log = Logger.getLogger(CommandManagerImpl.class);
      
   private final java.util.Queue<Packet> resendCache = new ConcurrentLinkedQueue<Packet>();
   
   private final int commandBatchSize;
   
   //These members don't need to be synchronized since only called by one thread concurrently
   //but do need to be volatile since that could be a different thread each time
   
   private volatile int lastCommandID = -1;
   
   private volatile int nextConfirmation;
   
   private volatile int commandIDSequence = 0;
   
   private final RemotingConnection remotingConnection;
   
   private final long sessionTargetID;
   
   private final long localCommandResponseTargetID;
   
   private final long remoteCommandResponseTargetID;
   
   public CommandManagerImpl(final int commandBatchSize, final RemotingConnection remotingConnection,
                             final PacketDispatcher dispatcher,
                             final long sessionTargetID,
                             final long localCommandResponseTargetID,
                             final long remoteCommandResponseTargetID)
   {
      this.commandBatchSize = commandBatchSize;
      
      nextConfirmation = commandBatchSize - 1;
      
      this.remotingConnection = remotingConnection;
      
      this.sessionTargetID = sessionTargetID;
      
      this.localCommandResponseTargetID = localCommandResponseTargetID;
      
      this.remoteCommandResponseTargetID = remoteCommandResponseTargetID;
      
      dispatcher.register(this);
   }
       
   //Needs to be synchronized since on the server messages can be sent to the client same time as blocking
   //responses are returned
   public synchronized Packet sendCommandBlocking(final long targetID, final Packet packet) throws MessagingException
   {
      setCommandID(packet);
      
      Packet response = remotingConnection.sendBlocking(targetID, sessionTargetID, packet, this);
        
      if (response.getType() == PacketImpl.EXCEPTION)
      {
         MessagingExceptionMessage mem = (MessagingExceptionMessage)response;
         
         throw mem.getException();
      }
      
      return response;
   }
   
   public synchronized void sendCommandOneway(final long targetID, final Packet packet)
   {
      setCommandID(packet);
      
      remotingConnection.sendOneWay(targetID, sessionTargetID, packet);
   }
    
   public void packetProcessed(final Packet packet)
   {
      long commandID = packet.getCommandID();
      
      if (commandID != ++lastCommandID)
      {
         throw new IllegalStateException("Command id out of sequence, got " + commandID + " expected " + lastCommandID);
      }
      
      if (commandID == nextConfirmation)
      {
         Packet confirmed = new PacketsConfirmedMessage(lastCommandID);
               
         confirmed.setTargetID(remoteCommandResponseTargetID);
         
         nextConfirmation += commandBatchSize;
          
         remotingConnection.sendOneWay(confirmed);
      }
   }
   
   public void close()
   {
      remotingConnection.getPacketDispatcher().unregister(localCommandResponseTargetID);
   }
         
   // PacketHandler implementation --------------------------------------------------------------
   
   public long getID()
   {
      return localCommandResponseTargetID;
   }

   public void handle(final Object connectionID, final Packet m)
   {      
      PacketsConfirmedMessage msg = (PacketsConfirmedMessage)m;
      
      Packet packet;
      do
      {
         packet = resendCache.poll();      
      }
      while (packet.getCommandID() < msg.getCommandID());
   }
   
   // Public -----------------------------------------------------------------------------------
   
   public int getUnconfirmedPackets()
   {
      return this.resendCache.size();
   }
   
   // Private -----------------------------------------------------------------------------------
   
   private void setCommandID(final Packet packet)
   {
      int commandID = commandIDSequence++;
      
      packet.setCommandID(commandID);
           
      resendCache.add(packet);
   }
    
}
