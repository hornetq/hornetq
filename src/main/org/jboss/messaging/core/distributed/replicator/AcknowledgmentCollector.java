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
import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jboss.logging.Logger;

import java.io.Serializable;

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

   // Constructors --------------------------------------------------

   public AcknowledgmentCollector(Serializable id)
   {
      this.id = id;
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

   public void startCollecting(CompositeDelivery d)
   {
      if (log.isTraceEnabled()) { log.trace(this + " starts collecting acknowledgments for " + d); }

   }

   public void remove(CompositeDelivery d)
   {
      if (log.isTraceEnabled()) { log.trace(this + " removing " + d); }

   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
