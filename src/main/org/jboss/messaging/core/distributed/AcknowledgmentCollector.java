/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.MessageListener;
import org.jboss.messaging.interfaces.Message;
import org.jboss.logging.Logger;

import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class AcknowledgmentCollector implements MessageListener
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicatorOutput.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   protected RpcDispatcher dispatcher;
   protected Serializable replicatorID;
   /** the original MessageListener of the RpcDispatcher, to which I am delegating now */
   protected MessageListener delegateListener;


   // Constructors --------------------------------------------------

   public AcknowledgmentCollector(RpcDispatcher dispatcher,  Serializable replicatorID)
   {
      this.dispatcher = dispatcher;
      this.replicatorID = replicatorID;
   }

   // MessageListener implementation --------------------------------

   public void receive(org.jgroups.Message jgroupsMessage)
   {
      Object  o = jgroupsMessage.getObject();
      Boolean ackType;
      if (o instanceof Message)
      {
         Message m = (Message)o;
         if (replicatorID.equals(m.getHeader(Message.REPLICATOR_ID)))
         {
            if ((ackType = (Boolean)m.getHeader(Message.ACKNOWLEGMENT_TYPE)) != null)
            {
               System.out.println("TODO acknowledgment received");
               // remove the message from the list of those wait for acknowledgment
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


   // Public --------------------------------------------------------

   /**
    * Atthaches the acknowledgment collector to a dispatcher. The collector output will
    * asynchronously handle messages received by the jChannel the dispatcher is connected to.
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


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
