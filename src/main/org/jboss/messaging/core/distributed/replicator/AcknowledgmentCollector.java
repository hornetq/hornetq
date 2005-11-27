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
package org.jboss.messaging.core.distributed.replicator;

import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.messaging.util.Util;
import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jboss.messaging.core.distributed.util.DelegatingMessageListener;
import org.jboss.messaging.core.distributed.util.DelegatingMessageListenerSupport;
import org.jboss.logging.Logger;
import org.jgroups.MessageListener;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class AcknowledgmentCollector implements AcknowledgmentCollectorFacade
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(AcknowledgmentCollector.class);

   public static final int DELIVERY_RETRIES = 5;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Serializable id;
   protected RpcDispatcher dispatcher;
   protected MessageListener collectorMessageListener;
   // <messageID - CompositeDelivery>
   protected Map deliveries;


   // Constructors --------------------------------------------------

   public AcknowledgmentCollector(Serializable id, RpcDispatcher dispatcher)
   {
      this.id = id;
      this.dispatcher = dispatcher;
      this.collectorMessageListener = null;
      deliveries = new HashMap();
   }

   // AcknowledgmentCollectorFacade implementation ------------------

   public Serializable getID()
   {
      return id;
   }

   public void acknowledge(PeerIdentity originator, Serializable messageID)
   {
      if (log.isTraceEnabled()) { log.trace(this + " receives acknowledgment from " + originator + " for " + messageID); }
      throw new NotYetImplementedException();
   }

   public void cancel(PeerIdentity originator, Serializable messageID)
   {
      if (log.isTraceEnabled()) { log.trace(this + " receives cancellation from " + originator + " for " + messageID); }
      throw new NotYetImplementedException();
   }

   // Public --------------------------------------------------------

   public synchronized void start()
   {
      if (collectorMessageListener != null)
      {
         // already started
         return;
      }

      // delegate to the existing listener, if any
      MessageListener delegateListener = dispatcher.getMessageListener();
      collectorMessageListener = new CollectorMessageListener(delegateListener);
      dispatcher.setMessageListener(collectorMessageListener);
   }

   public synchronized void stop()
   {
      if (collectorMessageListener == null)
      {
         return;
      }

      DelegatingMessageListener dl = (DelegatingMessageListener)dispatcher.getMessageListener();

      if (dl == collectorMessageListener)
      {
         dispatcher.setMessageListener(dl.getDelegate());
      }
      else
      {
         dl.remove(collectorMessageListener);
      }
   }

   public void startCollecting(CompositeDelivery d)
   {
      if (log.isTraceEnabled()) { log.trace(this + " starts collecting acknowledgments for " + d); }
      deliveries.put(d.getReference().getMessageID(), d);
   }

   public void remove(CompositeDelivery d)
   {
      if (log.isTraceEnabled()) { log.trace(this + " removing " + d); }
   }

   public String toString()
   {
      return "Collector[" + Util.guidToString(id) + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

   protected class CollectorMessageListener extends DelegatingMessageListenerSupport
   {
      // Constants -----------------------------------------------------

      // Static --------------------------------------------------------

      // Attributes ----------------------------------------------------

      // Constructors --------------------------------------------------

      public CollectorMessageListener(MessageListener delegate)
      {
         super(delegate);
      }

      // MessageListener implementation --------------------------------

      public void receive(org.jgroups.Message jgroupsMessage)
      {
         boolean handled = false;
         Object  o = jgroupsMessage.getObject();

         try
         {
            if (!(o instanceof Acknowledgment))
            {
               if (log.isTraceEnabled()) { log.trace(this + " discarding " + o); }
               return;
            }

            Acknowledgment ack = (Acknowledgment)o;

            if (log.isTraceEnabled()) { log.trace(this + " received " + ack); }

            Object messageID = ack.getMessageID();
            CompositeDelivery d = (CompositeDelivery)deliveries.get(messageID);

            if (d != null)
            {
               try
               {
                  handled = d.handle(ack);
               }
               catch(Throwable t)
               {
                  log.error("delivery failed to handle acknowledgment " + ack, t);
               }
            }
         }
         finally
         {
            if (!handled && delegate != null)
            {
               // forward the message to delegate
               delegate.receive(jgroupsMessage);
            }
         }
      }

      public byte[] getState()
      {
         if (delegate != null)
         {
            return delegate.getState();
         }
         return null;
      }

      public void setState(byte[] state)
      {
         if (delegate != null)
         {
            delegate.setState(state);
         }
      }

      // Public --------------------------------------------------------

      public String toString()
      {
         return AcknowledgmentCollector.this + ".Listner";
      }

      // Package protected ---------------------------------------------

      // Protected -----------------------------------------------------

      // Private -------------------------------------------------------

      // Inner classes -------------------------------------------------

   }
}
