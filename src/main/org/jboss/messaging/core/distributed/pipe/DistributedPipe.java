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

import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.distributed.util.RpcServerCall;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.util.Util;
import org.jboss.logging.Logger;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.Address;

import java.io.Serializable;

/**
 * The input end of a distributed pipe that synchronously forwards messages to a receiver in a
 * different address space. A distributed pipe <i>is not</i> a channel.
 *
 * The output end of the pipe its identified by a JGroups address and the pipe ID. Multiple
 * distributed pipes can share the same DistributedPipeOutput instance (and implicitly the pipeID),
 * as long the input instances are different.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DistributedPipe implements Receiver
 {
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DistributedPipe.class);

   private static final long TIMEOUT = 3600000;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Serializable id;
   protected RpcDispatcher dispatcher;
   protected Address outputAddress;

   // Constructors --------------------------------------------------

   public DistributedPipe(Serializable id, RpcDispatcher dispatcher, Address outputAddress)
   {
      this.dispatcher = dispatcher;
      this.outputAddress = outputAddress;
      this.id = id;
      log.debug(this + " created");
   }

   // Receiver implementation ---------------------------------------

   public Delivery handle(DeliveryObserver observer, Routable r, Transaction tx)
   {
      // TODO for the time being, this end always makes synchonous calls and always returns "done"
      //      deliveries. Syncronous/asynchronous calls should be configurable.

      // Check if the message was sent remotely; in this case, I must not resend it to avoid
      // endless loops among peers or deadlock on distributed RPC if deadlock detection is not
      // enabled.
      if (r.getHeader(Routable.REMOTE_ROUTABLE) != null)
      {
         if (log.isTraceEnabled()) { log.trace("rejecting remote routable " + r); }
         return null;
      }

      if (log.isTraceEnabled()) { log.trace(this + " handling " + r + (tx == null ? " non-transactionally" : " in transaction " + tx)); }

      // Convert a message reference back into a Message before sending remotely
      // TODO If the remote peer shares the same message store, there's no point converting dereferencing the message
      if (r.isReference())
      {
         MessageReference ref = (MessageReference)r;
         r = ref.getMessage();
      }

      try
      {
         return (Delivery)call("handle",
                               new Object[] {r},
                               new String[] {"org.jboss.messaging.core.Routable"});
      }
      catch(Throwable e)
      {
         log.error("Remote call handle() on " + this + " failed", e);
         return null;
      }
   }

   // Public --------------------------------------------------------

   public Address getOutputAddress()
   {
      return outputAddress;
   }

   public Serializable getID()
   {
      return id;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer();

      sb.append("DistributedPipe[");
      sb.append(Util.guidToString(id));
      sb.append(" -> ");
      sb.append(outputAddress);
      sb.append("]");
      return sb.toString();
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   /**
    * Synchronous remote call.
    */
   private Object call(String methodName, Object[] args, String[] argTypes) throws Throwable
   {
      if (outputAddress == null)
      {
         throw new IllegalStateException(this + " has a null output address");
      }

      RpcServerCall rpcServerCall =  new RpcServerCall(id, methodName, args, argTypes);

      // TODO use the timout when I'll change the send() signature or deal with the timeout
      return rpcServerCall.remoteInvoke(dispatcher, outputAddress, TIMEOUT);
   }


   // Inner classes -------------------------------------------------
}
