/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.interfaces.Receiver;
import org.jboss.messaging.interfaces.Message;
import org.jboss.logging.Logger;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.MessageListener;

import java.io.Serializable;


/**
 * The "receiving end" of a replicator. A replicator can have multiple inputs and multiple outputs.
 * Messages sent by each input are replicated to every output.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReplicatorOutput implements MessageListener
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicatorOutput.class);


   // Attributes ----------------------------------------------------

   protected Serializable replicatorID;
   protected Receiver receiver;

   protected MessageDispatcher dispatcher;
   /** the original MessageListener of the RpcDispatcher, to which I am delegating now */
   protected MessageListener delegateListener;

   // Constructors --------------------------------------------------

   /**
    * @param replicatorID - the id of the replicator. It must match the id used to instantiate the
    *        input ends of the pipe.
    */
   public ReplicatorOutput(Serializable replicatorID, Receiver receiver)
   {
      this.replicatorID = replicatorID;
      this.receiver = receiver;
   }


   // MessageListener implementation --------------------------------

   public void receive(org.jgroups.Message jgroupsMessage)
   {
      Object  o = jgroupsMessage.getObject();
      if (o instanceof Message)
      {
         Message m = (Message)o;
         if (replicatorID.equals(m.getHeader(Message.REPLICATOR_ID_HEADER)))
         {
            m.removeHeader(Message.REPLICATOR_ID_HEADER);
            // Mark the message as being received from a remote endpoint
            m.putHeader(Message.REMOTE_MESSAGE_HEADER, Message.REMOTE_MESSAGE_HEADER);
            boolean acked = false;
            try
            {
               acked = receiver.handle(m);
            }
            catch(Exception e)
            {
               log.warn(this + "'s receiver did not acknowledge the message", e);
               acked = false;
            }
            // TODO Deal with the acknowledgment - return an acknowledgment for the message.
            // TODO Sliding window?
         }
      }
      if (delegateListener != null)
      {
         delegateListener.receive(jgroupsMessage);
      }
   }

   public byte[] getState()
   {
      if (delegateListener != null)
      {
         return delegateListener.getState();
      }
      return null;
   }

   public void setState(byte[] state)
   {
      if (delegateListener != null)
      {
         delegateListener.setState(state);
      }
   }


   // Public --------------------------------------------------------

   public Serializable getID()
   {
      return replicatorID;
   }

   /**
    * Atthaches the replicator output to a dispatcher. The replicator output will asynchronously
    * handle messages received by the jChannel the dispatcher is connected to.
    */
   public void attach(RpcDispatcher d)
   {
      MessageListener l = d.getMessageListener();
      if (l != null)
      {
         delegateListener = l;
      }
      d.setMessageListener(this);
      this.dispatcher = d;
   }

   /**
    * Detaches the replicator from the dispatcher.
    */
   public void detach()
   {
      if (delegateListener != null)
      {
         dispatcher.setMessageListener(delegateListener);
      }
      delegateListener = null;
      dispatcher = null;
   }

   /**
    * @return the receiver connected to the replicator or null if there is no Receiver.
    */
   public Receiver getReceiver()
   {
      return receiver;
   }

   /**
    * Connect a receiver to the output end of the replicator.
    */
   public void setReceiver(Receiver r)
   {
       receiver = r;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("ReplicatorOutput[");
      sb.append(replicatorID);
      sb.append("]");
      return sb.toString();
   }


   // Static --------------------------------------------------------

   /** access it only from getUniqueID() */
   private static int sequence = 0;

   /**
    * Returns runtime ReplicatorOutput IDs that are unique per classloading domain.
    * @return an unique Integer.
    */
   synchronized static Integer getUniqueID()
   {
      return new Integer(sequence++);
   }

}


