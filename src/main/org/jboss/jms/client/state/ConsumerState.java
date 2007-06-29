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
package org.jboss.jms.client.state;

import java.util.Collections;

import org.jboss.jms.client.container.ClientConsumer;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.remoting.CallbackManager;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.messaging.util.Version;


/**
 * State corresponding to a Consumer. This state is acessible inside aspects/interceptors.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:ovidiu@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConsumerState extends HierarchicalStateSupport
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private int consumerID;
   private JBossDestination destination;
   private String selector;
   private String subscriptionName;
   private boolean noLocal;
   private boolean isConnectionConsumer;
   private ClientConsumer clientConsumer;
   private int bufferSize;
   private int maxDeliveries;
   private long redeliveryDelay;

   private boolean storingDeliveries;
   
   private SessionState parent;
   private ConsumerDelegate delegate;
   
   // Constructors ---------------------------------------------------------------------------------

   public ConsumerState(SessionState parent, ConsumerDelegate delegate, JBossDestination dest,
                        String selector, boolean noLocal, String subscriptionName, int consumerID,
                        boolean isCC, int bufferSize, int maxDeliveries, long redeliveryDelay)
   {
      super(parent, (DelegateSupport)delegate);
      children = Collections.EMPTY_SET;
      this.destination = dest;
      this.selector = selector;
      this.noLocal = noLocal;
      this.consumerID = consumerID;
      this.isConnectionConsumer = isCC;
      this.bufferSize = bufferSize;
      this.subscriptionName=subscriptionName;
      this.maxDeliveries = maxDeliveries;
      this.redeliveryDelay = redeliveryDelay;
    
      //We don't store deliveries if this a non durable subscriber
      
      if (dest.isTopic() && subscriptionName == null)
      {
         storingDeliveries = false;                 
      }
      else
      {
         storingDeliveries = true;
      }      
   }

   // HierarchicalState implementation -------------------------------------------------------------

   public DelegateSupport getDelegate()
   {
      return (DelegateSupport)delegate;
   }

   public void setDelegate(DelegateSupport delegate)
   {
      this.delegate = (ConsumerDelegate)delegate;
   }

   public HierarchicalState getParent()
   {
      return parent;
   }

   public void setParent(HierarchicalState parent)
   {
      this.parent=(SessionState)parent;
   }

   public Version getVersionToUse()
   {
      return parent.getVersionToUse();
   }

   // HierarchicalStateSupport overrides -----------------------------------------------------------

   public void synchronizeWith(HierarchicalState ns) throws Exception
   {
      ConsumerState newState = (ConsumerState)ns;

      int oldConsumerID = consumerID;
      consumerID = newState.consumerID;
      
      CallbackManager oldCallbackManager = ((ClientConnectionDelegate)getParent().getParent().
         getDelegate()).getRemotingConnection().getCallbackManager();
      CallbackManager newCallbackManager = ((ClientConnectionDelegate)ns.getParent().getParent().
         getDelegate()).getRemotingConnection().getCallbackManager();

      // We need to synchronize the old message callback handler using the new one

      ClientConsumer handler = oldCallbackManager.unregisterHandler(oldConsumerID);
      ClientConsumer newHandler = newCallbackManager.unregisterHandler(consumerID);
   
      handler.synchronizeWith(newHandler);
      newCallbackManager.registerHandler(consumerID, handler);
   }

   // Public ---------------------------------------------------------------------------------------

   public JBossDestination getDestination()
   {
      return destination;
   }

   public String getSelector()
   {
      return selector;
   }

   public boolean isNoLocal()
   {
      return noLocal;
   }

   public int getConsumerID()
   {
      return consumerID;
   }

   public boolean isConnectionConsumer()
   {
      return isConnectionConsumer;
   }

   public void setClientConsumer(ClientConsumer handler)
   {
      this.clientConsumer = handler;
   }

   public ClientConsumer getClientConsumer()
   {
      return clientConsumer;
   }

   public int getBufferSize()
   {
      return bufferSize;
   }

   public String getSubscriptionName()
   {
      return subscriptionName;
   }

   public void setSubscriptionName(String subscriptionName)
   {
      this.subscriptionName = subscriptionName;
   }

   public int getMaxDeliveries()
   {
      return maxDeliveries;
   }

   public boolean isStoringDeliveries()
   {
      return storingDeliveries;
   }
   
   public boolean isShouldAck()
   {
   	//If e are a non durable subscriber to a topic then there is no need
   	//to send acks to the server - we wouldn't have stored them on the server side anyway
   	
      return !(destination.isTopic() && subscriptionName == null);      
   }
   
   public long getRedeliveryDelay()
   {
   	return redeliveryDelay;
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
