/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.interfaces.Receiver;
import org.jboss.messaging.util.RpcServerCall;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.logging.Logger;
import org.jgroups.Address;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Serializable;

/**
 * The input end of a distributed pipe - a pipe that forwards messages to a remote receiver running
 * in a different address space than the sender.
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
 * @see org.jboss.messaging.interfaces.Receiver
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class PipeInput implements Receiver
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PipeInput.class);

   // Attributes ----------------------------------------------------

   protected boolean synchronous;
   protected RpcDispatcher dispatcher;
   protected Address outputAddress;
   protected Serializable pipeID;


   // Constructors --------------------------------------------------


   /**
    * PipeInput constructor.
    *
    * @param mode - true for synchronous behaviour, false otherwise.
    * @param dispatcher - the RPCDipatcher to delegate the transport to. The underlying JChannel
    *        doesn't necessarily have to be connected at the time the PipeInput is
    *        created.
    * @param outputAddress - the address of the group member the receiving end is available on.
    * @param pipeID - the unique identifier of the distributed pipe. This pipe's output end must
    *        be initialized with the same pipeID.
    */
   public PipeInput(boolean mode,
                    RpcDispatcher dispatcher,
                    Address outputAddress,
                    Serializable pipeID)
   {
      synchronous = mode;
      this.dispatcher = dispatcher;
      this.outputAddress = outputAddress;
      this.pipeID = pipeID;
      log.debug(this + " created");
   }

   // Receiver implementation ----------------------------------------

   public boolean handle(Message m)
   {
      if (synchronous)
      {
         return synchronousHandle(m);
      }
      else
      {
         return asynchronousHandle(m);
      }
   }

   // Public --------------------------------------------------------

   public boolean setSynchronous(boolean b)
   {
      synchronous = b;
      // TODO
      return true;
   }

   public boolean isSynchronous()
   {
      return synchronous;
   }

   public Address getOutputAddress()
   {
      return outputAddress;
   }

   public void setOutputAddress(Address a)
   {
      outputAddress = a;
   }

   public Serializable getID()
   {
      return pipeID;
   }

   public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("PipeInput[");
      sb.append(synchronous?"SYNCH":"ASYNCH");
      sb.append("][");
      sb.append(pipeID);
      sb.append("] -> ");
      sb.append(outputAddress);
      return sb.toString();
   }


   // Private -------------------------------------------------------

   private boolean synchronousHandle(Message m)
   {
      // Check if the message was sent remotely; in this case, I must not resend it to avoid
      // endless loops among peers or worse, deadlock on distributed RPC if deadlock detection
      // was not enabled.

      if (m.getHeader(Message.REMOTE_MESSAGE) != null)
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
                              new Object[] {m},
                              new String[] {"org.jboss.messaging.interfaces.Message"});

      try
      {
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

   private boolean asynchronousHandle(Message m)
   {
      throw new NotYetImplementedException();
   }

   // DEBUG ---------------------------------------------------------

   public String dump()
   {
      StringBuffer sb = new StringBuffer();
      sb.append(toString());
      sb.append(", messages: ");
      //sb.append(((MessageSetImpl)messages).dump());
      return sb.toString();
   }
}
