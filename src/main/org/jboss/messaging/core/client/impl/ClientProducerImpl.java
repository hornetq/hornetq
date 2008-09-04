/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.impl.wireformat.SendMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionProducerCloseMessage;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.TokenBucketLimiter;

/**
 * The client-side Producer connectionFactory class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
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
   
   private final int id;
   
   private final ClientSessionInternal session;
   
   private final Channel channel;
   
   private volatile boolean closed;
   
   //For limit throttling
   
   private Semaphore availableCredits;
   
   //For rate throttling
     
   private final TokenBucketLimiter rateLimiter;
   
   private final boolean blockOnNonPersistentSend;
   
   private final boolean blockOnPersistentSend;
   
   private final boolean creditFlowControl;
   
   private final int initialWindowSize;
    
   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------
      
   public ClientProducerImpl(final ClientSessionInternal session,                             
                             final int id,          
   		                    final SimpleString address,   		                   
   		                    final TokenBucketLimiter rateLimiter,
   		                    final boolean blockOnNonPersistentSend,
   		                    final boolean blockOnPersistentSend,
   		                    final int initialCredits,   		                      		                  
   		                    final Channel channel)
   {   	
      this.channel = channel;
      
      this.session = session;
      
      this.id = id;
      
      this.address = address;
      
      this.rateLimiter = rateLimiter;
            
      this.blockOnNonPersistentSend = blockOnNonPersistentSend; 
      
      this.blockOnPersistentSend = blockOnPersistentSend;
      
      this.availableCredits = new Semaphore(initialCredits);
      
      this.creditFlowControl = initialCredits != -1;
      
      this.initialWindowSize = initialCredits;
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
      
      try
      {
         channel.sendBlocking(new SessionProducerCloseMessage(id));
      }
      finally
      {      
         doCleanup();
      }
   }

   public void cleanUp()
   {
      if (closed)
      {
         return;         
      }
      
      doCleanup();
   }
         
   public boolean isClosed()
   {
      return closed;
   }
   
   public boolean isBlockOnPersistentSend()
   {
      return blockOnPersistentSend;
   }
   
   public boolean isBlockOnNonPersistentSend()
   {
      return blockOnNonPersistentSend;
   }
   
   public int getInitialWindowSize()
   {
      return initialWindowSize;
   }
   
   public int getMaxRate()
   {
      return rateLimiter == null ? -1 : rateLimiter.getRate();
   }
   
   // ClientProducerInternal implementation --------------------------------------------------------
   
   public int getID()
   {
      return id;
   }
   
   public void receiveCredits(final int credits)
   {
      availableCredits.release(credits);
   }
   
   public int getAvailableCredits()
   {
      return availableCredits.availablePermits();
   }
   
   // Public ---------------------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------
   
   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private void doCleanup()
   {
      session.removeProducer(this);

      closed = true;
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
      
      boolean sendBlocking = msg.isDurable() ? blockOnPersistentSend : blockOnNonPersistentSend;
      
      SendMessage message = new SendMessage(id, msg, sendBlocking);
               
      if (sendBlocking)
      {        
         channel.sendBlocking(message);
      }
      else
      {
         channel.send(message);
      }      
      
      //We only flow control with non-anonymous producers
      if (address == null && creditFlowControl)
      {
         try
         {
            availableCredits.acquire(message.getClientMessage().getEncodeSize());
         }
         catch (InterruptedException e)
         {           
         }         
      }
   }

   private void checkClosed() throws MessagingException
   {
      if (closed)
      {
         throw new MessagingException(MessagingException.OBJECT_CLOSED, "Producer is closed");
      }
   }

   // Inner Classes --------------------------------------------------------------------------------

}
