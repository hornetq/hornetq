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
package org.jboss.messaging.core.distributed.topic;

import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.plugin.contract.TransactionLogDelegate;
import org.jboss.messaging.core.ChannelSupport;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.distributed.replicator.Replicator;
import org.jboss.messaging.util.Util;
import org.jboss.logging.Logger;


/**
 * A local representative of a distributed topic. Each distributed topic peer instance has a remote
 * topic instance connected to its router.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
class RemoteTopic extends ChannelSupport

 {
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RemoteTopic.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   protected RemoteTopic(String topicName,
                         MessageStore ms,
                         TransactionLogDelegate tl,
                         Replicator replicator)
   {
      super(topicName + ".RemoteTopic", ms, tl, true);
      this.router = replicator;
   }

   // ChannelSupport overrides --------------------------------------

   public Delivery handle(DeliveryObserver sender, Routable r, Transaction tx)
   {
      // discard a routable coming from a remote peer
      if (r.getHeader(Routable.REMOTE_ROUTABLE) != null)
      {
         if (log.isTraceEnabled()) { log.trace(this + " discards remote message " + r); }
         return null;
      }
      return super.handle(sender, r, tx);
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "RemoteTopic[" + ((Replicator)router).getGroupID() + "." +
         Util.guidToString(((Replicator)router).getPeer().getPeerIdentity().getPeerID()) + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
