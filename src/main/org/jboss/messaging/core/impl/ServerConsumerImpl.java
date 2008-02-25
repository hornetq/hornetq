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
package org.jboss.messaging.core.impl;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.HandleStatus;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.PostOffice;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.ServerConsumer;
import org.jboss.messaging.core.ServerSession;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.util.Logger;

/**
 * Concrete implementation of a ClientConsumer. 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Partially derived from JBM 1.x version by:
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision: 3783 $</tt> $Id: ServerConsumerImpl.java 3783 2008-02-25 12:15:14Z timfox $
 */
public class ServerConsumerImpl implements ServerConsumer
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConsumerImpl.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private final boolean trace = log.isTraceEnabled();

   private final String id;

   private final Queue messageQueue;
   
   private final boolean noLocal;

   private final Filter filter;
   
   private final boolean autoDeleteQueue;
   
   private final boolean enableFlowControl;
   
   private final String connectionID;   
   
   private final ServerSession sessionEndpoint;

   private final PersistenceManager persistenceManager;
   
   private final PostOffice postOffice;
         
   private final Object startStopLock = new Object();

   private final AtomicInteger availableTokens = new AtomicInteger(0);
   
   private boolean started;

   // Constructors ---------------------------------------------------------------------------------

   ServerConsumerImpl(final Queue messageQueue, final boolean noLocal, final Filter filter,
   		                 final boolean autoDeleteQueue, final boolean enableFlowControl,
   		                 final String connectionID, final ServerSession sessionEndpoint,
					           final PersistenceManager persistenceManager, final PostOffice postOffice,
					           final boolean started)
   {
   	id = UUID.randomUUID().toString();
      
      this.messageQueue = messageQueue;
      
      this.noLocal = noLocal;

      this.filter = filter;
      
      this.autoDeleteQueue = autoDeleteQueue;
      
      this.enableFlowControl = enableFlowControl;
      
      this.connectionID = connectionID;

      this.sessionEndpoint = sessionEndpoint;

      this.persistenceManager = persistenceManager;
      
      this.postOffice = postOffice;
      
      this.started = started;
      
      messageQueue.addConsumer(this);
      
      messageQueue.deliver();
   }

   // ServerConsumer implementation ----------------------------------------------------------------------

   public String getID()
   {
   	return id;
   }
   
   public HandleStatus handle(MessageReference ref) throws Exception
   {
      if (enableFlowControl && availableTokens.get() == 0)
      {
         return HandleStatus.BUSY;
      }

      if (ref.getMessage().isExpired())
      {         
         ref.expire(persistenceManager);
         
         return HandleStatus.HANDLED;
      }
      
      synchronized (startStopLock)
      {
         // If the consumer is stopped then we don't accept the message, it should go back into the
         // queue for delivery later.
         if (!started)
         {
            return HandleStatus.BUSY;
         }
         
         Message message = ref.getMessage();
         
         if (filter != null && !filter.match(message))
         {
            return HandleStatus.NO_MATCH;
         }
         
         if (noLocal)
         {
            String conId = message.getConnectionID();

            if (connectionID.equals(conId))
            {	            	
            	ref.acknowledge(persistenceManager);
            	
             	return HandleStatus.HANDLED;
            }            
         }
                         
         if (enableFlowControl)
         {
            availableTokens.decrementAndGet();
         }
                   
         try
         {
         	sessionEndpoint.handleDelivery(ref, this);
         }
         catch (Exception e)
         {
         	log.error("Failed to handle delivery", e);
         	
         	started = false; // DO NOT return null or the message might get delivered more than once
         }
                          
         return HandleStatus.HANDLED;
      }
   }
   
   public void close() throws Exception
   {
      if (trace)
      {
         log.trace(this + " close");
      }
      
      setStarted(false);

      messageQueue.removeConsumer(this);
      
      if (autoDeleteQueue)
      {
         if (messageQueue.getConsumerCount() == 0)
         {  
            postOffice.removeBinding(messageQueue.getName());
            
            if (messageQueue.isDurable())
            {
               persistenceManager.deleteAllReferences(messageQueue);
            }
         }
      }
      
      sessionEndpoint.removeConsumer(id);           
   }
   
   public void setStarted(boolean started)
   {
      boolean useStarted;
      
      synchronized (startStopLock)
      {
         this.started = started;   
         
         useStarted = started;         
      }
      
      //Outside the lock
      if (useStarted)
      {
         promptDelivery();
      }
   }
   
   public void receiveTokens(int tokens) throws Exception
   {
      availableTokens.addAndGet(tokens);

      promptDelivery();      
   }

   // Public -----------------------------------------------------------------------------
     
   public String toString()
   {
      return "ConsumerEndpoint[" + id + "]";
   }
   
   // Private --------------------------------------------------------------------------------------

   private void promptDelivery()
   {
      sessionEndpoint.promptDelivery(messageQueue);
   } 
}
