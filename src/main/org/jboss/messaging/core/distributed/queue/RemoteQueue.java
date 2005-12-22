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
package org.jboss.messaging.core.distributed.queue;

import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.distributed.pipe.DistributedPipe;
import org.jboss.messaging.core.distributed.RemotePeer;
import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jboss.messaging.core.tx.Transaction;

import org.jboss.logging.Logger;

/**
 * A representative of a distributed queue peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RemoteQueue extends RemotePeer implements Receiver
 {
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RemoteQueue.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected DistributedPipe p;

   // Constructors --------------------------------------------------

   public RemoteQueue(PeerIdentity remotePeerIdentity, DistributedPipe p)
   {
      super(remotePeerIdentity);
      this.p = p;

      if (log.isTraceEnabled()) { log.trace("created queue remote receiver for " + remotePeerIdentity); }
   }

   // Receiver implementation ---------------------------------------

   public Delivery handle(DeliveryObserver observer, Routable routable, Transaction tx)
   {
      return p.handle(observer, routable, tx);
   }
   
   // Public --------------------------------------------------------

   public String toString()
   {
      return "RemoteQueue[" + remotePeerIdentity + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
