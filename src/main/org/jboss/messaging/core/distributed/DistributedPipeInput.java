/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.interfaces.Channel;
import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.interfaces.Receiver;
import org.jboss.logging.Logger;
import org.jgroups.Address;
import org.jgroups.TimeoutException;
import org.jgroups.SuspectedException;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.GroupRequest;

import java.io.Serializable;

/**
 * The input end of a distributed pipe - a distributed channel with only one output that forwards
 * messages to a remote receiver running in a different address space than the sender.
 * <p>
 * The output end of the distributed pipe its identified by a JGroups address. When configuring the
 * output end of the pipe, the remote end must use the same pipeID as the one used to configure the
 * pipe's input end.
 * <p>
 * The asynchronous/synchronous behaviour of the distributed pipe can be configured using
 * setSynchronous().
 *
 * @see org.jboss.messaging.interfaces.Receiver
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class DistributedPipeInput implements Receiver
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DistributedPipeInput.class);

   // Attributes ----------------------------------------------------

   protected boolean synchronous;
   protected RpcDispatcher dispatcher;
   protected Address outputAddress;
   protected Serializable pipeID;


   // Constructors --------------------------------------------------


   public DistributedPipeInput(boolean mode, RpcDispatcher dispatcher, Serializable senderID)
   {
      this(mode, dispatcher, null, senderID);
   }

   /**
    * DistributedPipeInput constructor.
    *
    * @param mode - true for synchronous behaviour, false otherwise.
    * @param dispatcher - the RPCDipatcher to delegate the transport to.
    * @param outputAddress - the address of the group member the receiving end is available on.
    * @param pipeID - the unique identifier of the distributed pipe. This pipe's output end must
    *        be initialized with the same pipeID.
    */
   public DistributedPipeInput(boolean mode,
                               RpcDispatcher dispatcher,
                               Address outputAddress,
                               Serializable pipeID)
   {
      synchronous = mode;
      this.dispatcher = dispatcher;
      this.outputAddress = outputAddress;
      this.pipeID = pipeID;
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

   public Serializable getPipeID()
   {
      return pipeID;
   }

   public String toString() {
      return "DistributedPipeInput["+pipeID+"] -> "+outputAddress;
   }


   // Private -------------------------------------------------------

   private boolean synchronousHandle(Message m)
   {

      // Check if the message was sent remotely; in this case, I must not resend it to avoid
      // endless loops among peers

      if (m.getHeader(Message.REMOTE_MESSAGE_HEADER) != null)
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
      MethodCall groupCall =
            new MethodCall("invoke",
                           new Object[] {pipeID,
                                         methodName,
                                         new Object[] {m},
                                         new String[] {"org.jboss.messaging.interfaces.Message"}},
                           new String[] {"java.io.Serializable",
                                         "java.lang.String",
                                         "[Ljava.lang.Object;",
                                         "[Ljava.lang.String;"});
      try
      {
         // TODO use the timout when I'll change the send() signature or deal with the timeout
         Object result = dispatcher.callRemoteMethod(outputAddress,
                                                     groupCall,
                                                     GroupRequest.GET_ALL,
                                                     2000);

         // TODO refine the semantics of the return
         if (result instanceof Throwable)
         {
            log.error("Remote RPC call " + methodName + "() on " + outputAddress +
                      " had thrown an exception when executing remotely", (Throwable)result);
            return false;
         }
         else if (result instanceof Boolean)
         {
            return ((Boolean)result).booleanValue();
         }
         else
         {
            // unexpected result
            log.warn("Remote call " + methodName + "() on " + outputAddress +
                     " got unexpected result: " + result);
            return false;
         }
      }
      catch(TimeoutException e)
      {
         log.warn("Remote call " + methodName + "() on " + outputAddress +
                  " timed out: " + e);
         return false;
      }
      catch(SuspectedException e)
      {
         log.warn("Remote call " + methodName + "() on " + outputAddress +
                  " encountered suspected member: " + e);
         return false;
      }
   }

   private boolean asynchronousHandle(Message m)
   {
      // TODO
      return false;
   }

   // DEBUG ---------------------------------------------------------

   public String dump()
   {
      StringBuffer sb =
            new StringBuffer("DistributedPipeInput[").append(synchronous?"SYNCHRONOUS":"ASYNCHRONOUS").
            append("][").append(pipeID).append("] -> ").append(getOutputAddress()).
            append(", messages: ");
      //sb.append(((MessageSetImpl)messages).dump());
      return sb.toString();
   }
}
