/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.util.RpcServerCall;
import org.jboss.messaging.core.local.SingleOutputChannelSupport;
import org.jboss.logging.Logger;
import org.jgroups.Address;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Serializable;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * The input end of a distributed pipe - a channel with only one output that forwards messages to a
 * remote receiver running in a different address space than the sender.
 * <p>
 * The output end of the distributed pipe its identified by a JGroups address. When configuring the
 * output end of the pipe, the remote end must use the same pipeID as the one used to configure the
 * pipe's input end.
 * <p>
 * The asynchronous/synchronous behaviour of the distributed pipe can be configured using
 * setSynchronous().
 * <p>
 * Multiple distributed pipes can share the same PipeOutput instance (and implicitly the
 * pipeID), as long the DistributedPipeIntput instances are different.
 *
 * @see org.jboss.messaging.core.Receiver
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class Pipe extends SingleOutputChannelSupport
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Pipe.class);

   // Attributes ----------------------------------------------------

   protected RpcDispatcher dispatcher;
   protected Address outputAddress;
   protected Serializable pipeID;

   // Constructors --------------------------------------------------

   /**
    * Pipe constructor.
    *
    * @param mode - true for synchronous behaviour, false otherwise.
    * @param dispatcher - the RPCDipatcher to delegate the transport to. The underlying JChannel
    *        doesn't necessarily have to be connected at the time the Pipe is
    *        created.
    * @param outputAddress - the address of the group member the receiving end is available on.
    * @param pipeID - the unique identifier of the distributed pipe. This pipe's output end must
    *        be initialized with the same pipeID.
    */
   public Pipe(boolean mode,
               RpcDispatcher dispatcher,
               Address outputAddress,
               Serializable pipeID)
   {
      super(mode);
      this.dispatcher = dispatcher;
      this.outputAddress = outputAddress;
      this.pipeID = pipeID;
      unacked = new ArrayList();
      log.debug(this + " created");
   }

   // ChannelSupport implementation ---------------------------------

   public Serializable getReceiverID()
   {
      return pipeID;
   }

   public boolean handle(Routable r)
   {
      if (handleSynchronously(r))
      {
         // successful synchronous delivery
         return true;
      }
      return storeNACKedMessage(r, pipeID);
   }

   public boolean deliver()
   {
      lock();

      if (log.isTraceEnabled()) { log.trace("asynchronous delivery triggered on " + getReceiverID()); }

      try
      {
         // try to flush the message store
         for(Iterator i = unacked.iterator(); i.hasNext(); )
         {
            Routable r = (Routable)i.next();

            if (System.currentTimeMillis() > r.getExpirationTime())
            {
               // message expired
               log.warn("Message " + r.getMessageID() + " expired by " + (System.currentTimeMillis() - r.getExpirationTime()) + " ms");
               i.remove();
               continue;
            }

            if (handleSynchronously(r))
            {
               i.remove();
            }
            else
            {
               // most likely the receiver is broken, don't insist
               break;
            }
         }
         return unacked.isEmpty();
      }
      finally
      {
         unlock();
      }
   }

   // Public --------------------------------------------------------

   public Address getOutputAddress()
   {
      return outputAddress;
   }

   public void setOutputAddress(Address a)
   {
      outputAddress = a;
   }

   public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("Pipe[");
      sb.append(isSynchronous()?"SYNCH":"ASYNCH");
      sb.append("][");
      sb.append(pipeID);
      sb.append("] -> ");
      sb.append(outputAddress);
      return sb.toString();
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private boolean handleSynchronously(Routable r)
   {
      // Check if the message was sent remotely; in this case, I must not resend it to avoid
      // endless loops among peers or worse, deadlock on distributed RPC if deadlock detection
      // was not enabled.

      if (r.getHeader(Routable.REMOTE_ROUTABLE) != null)
      {
         // don't send
         return false;
      }

      if (outputAddress == null)
      {
         // A distributed pipe must be configured with a valid output address
         log.error(this + " has a null output address.");
         return false;
      }

      String methodName = "handle";
      RpcServerCall rpcServerCall =
            new RpcServerCall(pipeID,
                              methodName,
                              new Object[] {r},
                              new String[] {"org.jboss.messaging.core.Routable"});

      try
      {
         // call on the PipeOutput unique server delegate
         return ((Boolean)rpcServerCall.remoteInvoke(dispatcher, outputAddress, 30000)).
               booleanValue();
      }
      catch(Throwable e)
      {
         log.error("Remote call " + methodName + "() on " + outputAddress +  "." + pipeID +
                  " failed", e);
         return false;
      }
   }
}
