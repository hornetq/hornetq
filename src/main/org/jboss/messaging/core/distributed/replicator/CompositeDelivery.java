/**
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

import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jboss.logging.Logger;

import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class CompositeDelivery implements MultipleReceiversDelivery
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(CompositeDelivery.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected DeliveryObserver observer;
   protected MessageReference reference;
   // <PeerIdentity>
   protected Set outputIdentities;
   protected boolean cancelOnMessagesRejection;
   protected boolean cancelled;
   protected boolean done;

   // Constructors --------------------------------------------------

   public CompositeDelivery(DeliveryObserver observer, 
                            MessageReference reference, 
                            boolean cancelOnMessagesRejection)
   {
      this(observer, reference,cancelOnMessagesRejection, null);
   }


   public CompositeDelivery(DeliveryObserver observer,
                            MessageReference reference,
                            boolean cancelOnMessagesRejection,
                            Set outputs)
   {
      this.observer = observer;
      this.reference = reference;
      this.cancelOnMessagesRejection = cancelOnMessagesRejection;
      if (outputs == null)
      {
         outputIdentities = new HashSet();
      }
      else
      {
         outputIdentities = outputs;
      }
      cancelled = false;
      done = false;
   }

   // MultipleReceiversDelivery implementation ---------------------

   public boolean cancelOnMessageRejection()
   {
      return cancelOnMessagesRejection;
   }

   public boolean handle(Acknowledgment ack) throws Throwable
   {
      boolean handled = false;
      Object messageID = ack.getMessageID();

      if (!reference.getMessageID().equals(messageID))
      {
         // not my acknowledgment, ignore it
         log.debug(this + " discarding acknowledgment for unknown message: " + ack);
      }

      if (log.isTraceEnabled()) { log.trace(this + " handling " + ack); }

      Object outputID = ack.getReplicatorOutputID();

      for(Iterator i = outputIdentities.iterator(); i.hasNext();)
      {
         PeerIdentity pid = (PeerIdentity)i.next();
         if (pid.getPeerID().equals(outputID))
         {

            int state = ack.getState();
            if (state == Acknowledgment.REJECTED && cancelOnMessagesRejection ||
                state == Acknowledgment.CANCELLED)
            {
               if (log.isTraceEnabled()) { log.trace(this + " cancelled"); }
               cancelled = true;
               break;
            }

            i.remove();
            handled = true;
            if (log.isTraceEnabled()) { log.trace(this + " expecting acknowledgments from " + outputIdentities.size() + " more peers"); }
            break;
         }
      }

      if (cancelled)
      {
         cancelDelivery();
      }

      if (outputIdentities.isEmpty())
      {
         acknowledgeDelivery();
      }

      return handled;
  }

   public MessageReference getReference()
   {
      return reference;
   }

   public boolean isDone()
   {
      return done;
   }

   public boolean isCancelled()
   {
      return cancelled;
   }

   public void setObserver(DeliveryObserver observer)
   {
      this.observer = observer;
   }

   public DeliveryObserver getObserver()
   {
      return observer;
   }

   public void add(Object receiver)
   {
      if (!(receiver instanceof PeerIdentity))
      {
         throw new IllegalArgumentException("The argument should be a PeerIdentity");
      }

      outputIdentities.add(receiver);
   }

   public boolean remove(Object receiver)
   {
      if (!(receiver instanceof PeerIdentity))
      {
         throw new IllegalArgumentException("The argument should be a PeerIdentity");
      }

      return outputIdentities.remove(receiver);
   }

   public Iterator iterator()
   {
      return outputIdentities.iterator();
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "CompositeDelivery[" + reference.getMessageID()+ ", " + outputIdentities.size() + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private void cancelDelivery()
   {
      cancelled = true;
      try
      {
         if (log.isTraceEnabled()) { log.trace(this + " cancelling on observer"); }
         observer.cancel(this);
      }
      catch(Throwable t)
      {
         log.error("Failed to cancel", t);
      }
   }

   private void acknowledgeDelivery()
   {
      done = true;
      if (log.isTraceEnabled()) { log.trace(this + " acknowledging on observer"); }
      observer.acknowledge(this, null);

   }
   
   // Inner classes -------------------------------------------------
}
