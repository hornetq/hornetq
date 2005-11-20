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
package org.jboss.messaging.core.distributed.pipe;

import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.util.Util;
import org.jboss.logging.Logger;

import java.io.Serializable;

/**
 * The "receiving end" of a distributed pipe.
 * <p>
 * Listens on a RpcDispatcher and synchronously handles messages sent by the input end of the
 * distributed pipe. Multiple distributed pipes can share the same DistributedPipeOutput instance
 * (and implicitly the pipeID), as long input instances are different.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DistributedPipeOutput implements PipeOutputFacade
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DistributedPipeOutput.class);


   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Serializable pipeID;
   protected Receiver receiver;

   // Constructors --------------------------------------------------

   /**
    * @param pipeID - the id of the distributed pipe. It must match the id used to instantiate the
    *        input end of the pipe.
    */
   public DistributedPipeOutput(Serializable pipeID, Receiver receiver)
   {
      this.pipeID = pipeID;
      this.receiver = receiver;
   }

   // PipeOutputFacade implementation -------------------------------

   public Serializable getID()
   {
      return pipeID;
   }

   public Delivery handle(Routable r)
   {
      if (log.isTraceEnabled()) { log.trace(this + " handles " + r); }

      // Mark the message as being received from a remote endpoint
      r.putHeader(Routable.REMOTE_ROUTABLE, Routable.REMOTE_ROUTABLE);

      Delivery d = receiver.handle(null, r, null);

      // we only accept synchronous deliveries at this point
      if (d != null && !d.isDone())
      {
         throw new IllegalStateException("Cannot handle asynchronous deliveries at this end");
      }

      if (log.isTraceEnabled()) { log.trace(this + " is relaying " + d); }
      return d;
   }

   // Public --------------------------------------------------------

   public Receiver getReceiver()
   {
      return receiver;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("DistributedPipeOutput[");
      sb.append(Util.guidToString(pipeID));
      sb.append("]");
      return sb.toString();
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
