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
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.util.Util;
import org.jboss.logging.Logger;

import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ReplicatorOutputDelivery extends SimpleDelivery
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicatorOutputDelivery.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Serializable replicatorOutputID;
   protected boolean cancelOnMessageRejection;

   // Constructors --------------------------------------------------

   public ReplicatorOutputDelivery(DeliveryObserver observer, 
                                   MessageReference reference,
                                   Serializable replicatorOutputID,
                                   boolean cancelOnMessageRejection)
   {
      super(observer, reference);
      this.replicatorOutputID = replicatorOutputID;
      this.cancelOnMessageRejection = cancelOnMessageRejection;
   }


   // Public --------------------------------------------------------

   public Serializable getOutputID()
   {
      return replicatorOutputID;
   }

   public boolean handle(Acknowledgment ack) throws Throwable
   {
      boolean handled = false;

      Object messageID = ack.getMessageID();
      Object outputID = ack.getReplicatorOutputID();

      if (!reference.getMessageID().equals(messageID) ||
          !replicatorOutputID.equals(outputID))
      {
         // not my acknowledgment, ignore it
         log.debug(this + " discarding acknowledgment for unknown message: " + ack);
      }

      handled = true;
      if (log.isTraceEnabled()) { log.trace(this + " handling " + ack); }

      int state = ack.getState();

      if (state == Acknowledgment.CANCELLED ||
         state == Acknowledgment.REJECTED && cancelOnMessageRejection)
      {
         if (log.isTraceEnabled()) { log.trace(this + " cancelled"); }
         cancelled = true;
         observer.cancel(this);
      }
      else
      {
         if (log.isTraceEnabled()) { log.trace(this + " positively acknowledged"); }
         done = true;
         observer.acknowledge(this, null);
      }

      return handled;
   }

   public String toString()
   {

      return "ReplicatorOutputDelivery[" + reference + ", " + Util.guidToString(replicatorOutputID) + "](" +
         (cancelled ? "cancelled" : done ? "done" : "active") + ")";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
