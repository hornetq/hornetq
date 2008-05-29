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
package org.jboss.messaging.core.client.impl;

import java.util.concurrent.Semaphore;

import org.jboss.messaging.core.client.AcknowledgementHandler;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerSendMessage;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.TokenBucketLimiter;

/**
 * The client-side Producer connectionFactory class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientProducerImpl implements ClientProducerInternal
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientProducerImpl.class);

   // Attributes -----------------------------------------------------------------------------------

   private boolean trace = log.isTraceEnabled();
   
   private final SimpleString address;
   
   private final long serverTargetID;
   
   private final long clientTargetID;
   
   private final ClientSessionInternal session;
   
   private final RemotingConnection remotingConnection;
   
   private volatile boolean closed;
   
   //For limit throttling
   
   private Semaphore availableCredits;
   
   //For rate throttling
     
   private final TokenBucketLimiter rateLimiter;
   
   private final boolean sendNonPersistentMessagesSynchronously;
   
   private final boolean sendPersistentMessagesSynchronously;
   
   private final boolean creditFlowControl;
     
   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------
      
   public ClientProducerImpl(final ClientSessionInternal session, final long serverTargetID,
                             final long clientTargetID,
   		                    final SimpleString address,
   		                    final RemotingConnection remotingConnection,
   		                    final int maxRate,
   		                    final boolean sendNonPersistentMessagesSynchronously,
   		                    final boolean sendPersistentMessagesSynchronously,
   		                    final int initialCredits)
   {   	
      this.session = session;
      
      this.serverTargetID = serverTargetID;
      
      this.clientTargetID = clientTargetID;
      
      this.address = address;
      
      this.remotingConnection = remotingConnection;
      
      if (maxRate != -1)
      {
      	this.rateLimiter = new TokenBucketLimiter(maxRate, false);
      }
      else
      {
      	this.rateLimiter = null;
      }
      
      this.sendNonPersistentMessagesSynchronously = sendNonPersistentMessagesSynchronously; 
      
      this.sendPersistentMessagesSynchronously = sendPersistentMessagesSynchronously;
      
      log.info("send np:" + this.sendNonPersistentMessagesSynchronously + " p:" + this.sendPersistentMessagesSynchronously);
      
      this.availableCredits = new Semaphore(initialCredits);
      
      this.creditFlowControl = initialCredits != -1;
   }
   
   // ClientProducer implementation ----------------------------------------------------------------

   public SimpleString getAddress()
   {
   	return address;
   }
   
   public void send(final ClientMessage msg) throws MessagingException
   {
      checkClosed();
      
      doSend(null, msg);
   }
   
   public void send(final SimpleString address, final ClientMessage msg) throws MessagingException
   {
      checkClosed();
      
      doSend(address, msg);
   }
   
   private void doSend(final SimpleString address, final ClientMessage msg) throws MessagingException
   {
      if (address != null)
      {
         msg.setDestination(address);
      }
      else
      {
         msg.setDestination(this.address);
      }
         	   	
   	if (rateLimiter != null)
      {
         // Rate flow control
                  
         rateLimiter.limit();
      }
   	
   	boolean sendBlocking = msg.isDurable() && sendPersistentMessagesSynchronously ||
   	                       !msg.isDurable() && sendNonPersistentMessagesSynchronously;
   	
      ProducerSendMessage message = new ProducerSendMessage(msg);
         		
   	if (sendBlocking)
   	{
   	   remotingConnection.sendBlocking(serverTargetID, session.getServerTargetID(), message);
   	}
   	else
   	{
   	   remotingConnection.sendOneWay(serverTargetID, session.getServerTargetID(), message);
   	}   	 
   	
      //We only flow control with non-anonymous producers
      if (address == null && creditFlowControl)
      {
         try
         {
            this.availableCredits.acquire(message.getClientMessage().encodeSize());
         }
         catch (InterruptedException e)
         {           
         }         
      }
   }
            
   public void registerAcknowledgementHandler(final AcknowledgementHandler handler)
   {
      // TODO      
   }

   public void unregisterAcknowledgementHandler(final AcknowledgementHandler handler)
   {
      // TODO  
   }

   public synchronized void close() throws MessagingException
   {
      if (closed)
      {
         return;         
      }
      
      session.removeProducer(this);
      
      remotingConnection.getPacketDispatcher().unregister(clientTargetID);
      
      closed = true;
   }

   public boolean isClosed()
   {
      return closed;
   }
   
   // ClientProducerInternal implementation --------------------------------------------------------
   
   public void receiveCredits(final int credits)
   {
      availableCredits.release(credits);
   }
   
   // Public ---------------------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------
   
   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void checkClosed() throws MessagingException
   {
      if (closed)
      {
         throw new MessagingException(MessagingException.OBJECT_CLOSED, "Producer is closed");
      }
   }

   // Inner Classes --------------------------------------------------------------------------------

}
