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

import org.jboss.messaging.core.client.AcknowledgementHandler;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerSendMessage;
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
   
   private final String address;
   
   private final String id;
   
   private final ClientSessionInternal session;
   
   private final RemotingConnection remotingConnection;
   
   private volatile boolean closed;
   
   //For limit throttling
   
   private volatile int windowSize;
   
   //For rate throttling
     
   private final TokenBucketLimiter rateLimiter;
     
   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------
      
   public ClientProducerImpl(final ClientSessionInternal session, final String id, final String address,
   		                    final RemotingConnection remotingConnection, final int windowSize,
   		                    final int maxRate)
   {   	
      this.session = session;
      
      this.id = id;
      
      this.address = address;
      
      this.remotingConnection = remotingConnection;
      
      this.windowSize = windowSize;
      
      if (maxRate != -1)
      {
      	this.rateLimiter = new TokenBucketLimiter(maxRate, false);
      }
      else
      {
      	this.rateLimiter = null;
      }
   }
   
   // ClientProducer implementation ----------------------------------------------------------------

   public String getAddress()
   {
   	return address;
   }
   
   public void send(final Message msg) throws MessagingException
   {
      checkClosed();
      
      doSend(null, msg);
   }
   
   public void send(final String address, final Message msg) throws MessagingException
   {
      checkClosed();
      
      doSend(address, msg);
   }
   
   private void doSend(final String address, final Message msg) throws MessagingException
   {
   	ProducerSendMessage message = new ProducerSendMessage(address, msg.copy());
   	
   	//We only flow control with non-anonymous producers
   	if (address == null)
   	{
   		while (windowSize == 0)
   		{
				synchronized (this)
				{
					try
					{						
					   wait();						
					}
					catch (InterruptedException e)
					{   						
					}
				}		
   		}
   		
   		windowSize--;
   	}
   	
   	remotingConnection.send(id, message, !msg.isDurable());
   	 	   	
   	if (rateLimiter != null)
   	{
   	   // Rate flow control
      	   		
   		rateLimiter.limit();
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
      
      remotingConnection.getPacketDispatcher().unregister(id);
      
      closed = true;
   }

   public boolean isClosed()
   {
      return closed;
   }
   
   // ClientProducerInternal implementation --------------------------------------------------------
   
   public synchronized void receiveTokens(int tokens)
   {
   	windowSize += tokens;
   		
   	notify();
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
