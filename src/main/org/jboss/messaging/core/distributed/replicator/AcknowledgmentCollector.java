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
import java.util.Set;
import java.util.Iterator;

/**
 * TODO: thread safety, synchronize. Right now is wide open.
 *
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
   protected Serializable groupID;
   protected RpcDispatcher dispatcher;
   protected MessageListener collectorMessageListener;

   // TODO this data structure is far from being efficient. Refactor.
   // Map<messageID - Map<peerIdentity - ReplicatorOutputDelivery>>
   protected Map deliveries;


   // Constructors --------------------------------------------------

   public AcknowledgmentCollector(Serializable groupID, Serializable id, RpcDispatcher dispatcher)
   {
      this.id = id;
      this.groupID = groupID;
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

      // Not really used, as acknowledgment is done asyncronously
      throw new NotYetImplementedException();
   }

   public void cancel(PeerIdentity originator, Serializable messageID)
   {
      if (log.isTraceEnabled()) { log.trace(this + " receives cancellation from " + originator + " for " + messageID); }

      // Not really used, as cancellation is done asyncronously
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

   /**
    * @param dels - Set<ReplicatorOutputDelivery>.
    */
   public void startCollecting(Set dels)
   {

      for(Iterator i = dels.iterator(); i.hasNext(); )
      {
         ReplicatorOutputDelivery d = (ReplicatorOutputDelivery)i.next();
         Object messageID = new Long(d.getReference().getMessageID());

         Map perMessageMap = (Map)deliveries.get(messageID);
         if (perMessageMap == null)
         {
            perMessageMap = new HashMap();
            deliveries.put(messageID, perMessageMap);
         }

         Object outputID = d.getReceiverID();
         perMessageMap.put(outputID, d);
         if (log.isTraceEnabled()) { log.trace(this + " ready to collect acknowledgment for " + messageID + " from " + Util.guidToString(outputID)); }
      }
   }

   /**
    * @param dels - Set<ReplicatorOutputDelivery>.
    */
   public void remove(Set dels)
   {
      for(Iterator i = dels.iterator(); i.hasNext(); )
      {
         remove((ReplicatorOutputDelivery)i.next());
      }
   }

   public String toString()
   {
      return "Collector[" + groupID + "." + Util.guidToString(id) + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private ReplicatorOutputDelivery remove(ReplicatorOutputDelivery d)
   {
      Object messageID = new Long(d.getReference().getMessageID());
      Map perMessageMap = (Map)deliveries.get(messageID);

      if (perMessageMap == null)
      {
         return null;
      }

      ReplicatorOutputDelivery rem = (ReplicatorOutputDelivery)perMessageMap.remove(d.getReceiverID());

      if (log.isTraceEnabled()) { log.trace(rem == null ? "no such delivery to remove: " + d : "removed " + d); }

      // very unlikely to have to deal with the same message again, so don't leave uncollectable garbage
      if (perMessageMap.isEmpty())
      {
         if (log.isTraceEnabled()) { log.trace("all pending acknowledgments for message " + messageID + " received"); }
         deliveries.remove(messageID);
      }

      return rem;
   }

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
            Object outputID = ack.getReplicatorOutputID();

            Map perMessageMap = (Map)deliveries.get(messageID);

            if (perMessageMap == null)
            {
               if (log.isTraceEnabled()) { log.trace(this + ": no deliveries for message " + messageID); }
               return;
            }

            ReplicatorOutputDelivery d = (ReplicatorOutputDelivery)perMessageMap.get(outputID);

            if (d == null)
            {
               if (log.isTraceEnabled()) { log.trace(this + ": no deliveries waiting for output " + Util.guidToString(outputID)); }
               return;
            }

            try
            {
               handled = d.handle(ack);
               AcknowledgmentCollector.this.remove(d);
            }
            catch(Throwable t)
            {
               log.error("delivery failed to handle acknowledgment " + ack, t);
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
