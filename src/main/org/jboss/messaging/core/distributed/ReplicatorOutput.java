/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.interfaces.Receiver;
import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.core.CoreMessage;
import org.jboss.logging.Logger;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.MessageListener;

import java.io.Serializable;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;


/**
 * The "receiving end" of a replicator. A replicator can have multiple inputs and multiple outputs.
 * Messages sent by each input are replicated to every output.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReplicatorOutput implements MessageListener, Runnable
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicatorOutput.class);

   // Attributes ----------------------------------------------------

   protected Serializable replicatorID;
   protected Receiver receiver;

   protected MessageDispatcher dispatcher;
   /** the original MessageListener of the RpcDispatcher, to which I am delegating now */
   protected MessageListener delegateListener;

   protected LinkedQueue acknowledgmentQueue;
   protected Thread acknowledgmentThread;

   // Constructors --------------------------------------------------

   /**
    * @param replicatorID - the id of the replicator. It must match the id used to instantiate the
    *        input ends of the pipe.
    */
   public ReplicatorOutput(Serializable replicatorID, Receiver receiver)
   {
      this.replicatorID = replicatorID;
      this.receiver = receiver;
      acknowledgmentQueue = new LinkedQueue();
      acknowledgmentThread = new Thread(this);
      acknowledgmentThread.start();
   }

   // MessageListener implementation --------------------------------

   public void receive(org.jgroups.Message jgroupsMessage)
   {
      Object  o = jgroupsMessage.getObject();
      if (o instanceof Message)
      {
         Message m = (Message)o;
         if (replicatorID.equals(m.getHeader(Message.REPLICATOR_ID)))
         {
            // see if it's an acknowledgment for a AcknowledgmentCollector using the same
            // MessageDispatcher
            if (m.getHeader(Message.ACKNOWLEGMENT_TYPE) == null)
            {
               m.removeHeader(Message.REPLICATOR_ID);
               // Mark the message as being received from a remote endpoint
               m.putHeader(Message.REMOTE_MESSAGE, Message.REMOTE_MESSAGE);
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
               while(true)
               {
                  try
                  {
                     acknowledgmentQueue.put(new MessageAcknowledgment(jgroupsMessage.getSrc(),
                                                                       m.getID(), acked));
                     break;
                  }
                  catch(InterruptedException e)
                  {
                     log.warn("Thread interrupted while trying to put an acknowledgment in queue", e);
                  }
               }
            }
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

   // Runnable implementation ---------------------------------------

   /**
    * Runs on the acknowledgment thread.
    */
   public void run()
   {
      while(true)
      {
         try
         {
            MessageAcknowledgment ack = (MessageAcknowledgment)acknowledgmentQueue.take();
            acknowledge(ack);
         }
         catch(InterruptedException e)
         {
            log.warn("Thread interrupted while trying to take an acknowledgment from queue", e);
         }
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
      log.debug(this + " attached to " + d);
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
      log.debug(this + " detached from " + dispatcher);
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

   // Protected -----------------------------------------------------

   /**
    * TODO incomplete implementation
    *
    * Positively or negatively acknowledge a message to the sender.
    */
   protected void acknowledge(MessageAcknowledgment ack)
   {
      int RETRIES = 3;

      // TODO VERY inefficient and actually not very correct implementation: even if the
      // TODO message is correctly sent, I don't know it will reach the ReceiverInput. Better
      // TODO use a synchronous call?
      // TODO Sliding Window?
      org.jgroups.Channel jChannel = dispatcher.getChannel();
      Message m = new CoreMessage(generateMessageID());
      m.putHeader(Message.REPLICATOR_ID, replicatorID);
      m.putHeader(Message.ACKNOWLEDGED_MESSAGE_ID, ack.getMessageID());
      m.putHeader(Message.ACKNOWLEGMENT_TYPE, ack.isPositive());
      int cnt = 0;
      Exception exception = null;
      while(cnt++ < RETRIES)
      {
         try
         {
            jChannel.send(ack.getSender(), null, m);
            // TODO: turn it in a TRACE statement
            if (log.isDebugEnabled()) { log.debug(ack + "sent"); }
            return;
         }
         catch(Exception e)
         {
            exception = e;
         }
      }

      log.warn("Failed to send acknowledgment after " + cnt + " retries", exception);

      // resubmit the acknowlegment to the queue
      while(true)
      {
         try
         {
            acknowledgmentQueue.put(ack);
         }
         catch(InterruptedException e)
         {
            log.warn("Thread interrupted while trying to put an acknowledgment in queue", e);
         }
      }
   }


   protected Serializable generateMessageID()
   {
      return null;
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


