/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Distributor;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.jms.util.JBossJMSException;

import javax.jms.Message;
import javax.jms.JMSException;
import java.io.Serializable;

import EDU.oswego.cs.dl.util.concurrent.BoundedChannel;
import EDU.oswego.cs.dl.util.concurrent.BoundedLinkedQueue;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerConsumerDelegate implements ConsumerDelegate, Receiver
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConsumerDelegate.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String id;
   protected Distributor destination;
   protected ServerSessionDelegate parent;

   protected BoundedChannel deliveryQueue;

   // Constructors --------------------------------------------------

   public ServerConsumerDelegate(String id, Distributor destination, ServerSessionDelegate parent)
   {
      this.id = id;
      this.destination = destination;
      this.parent = parent;

      deliveryQueue = new BoundedLinkedQueue(10);

      // register myself with the destination
      destination.add(this);
   }

   // ConsumerDelegate implementation -------------------------------

   public Message receive(long timeout) throws JMSException
   {
      try
      {
         return (Message)deliveryQueue.poll(timeout);
      }
      catch(Exception e)
      {
         throw new JBossJMSException("Failed to read from the delivery queue", e);
      }
   }

   // Receiver implementation ---------------------------------------

   public Serializable getReceiverID()
   {
      return id;
   }

   public boolean handle(Routable r)
   {
      if (log.isTraceEnabled()) { log.trace("Receiving routable " + r); }

      try
      {
         return deliveryQueue.offer(r, 500);
      }
      catch(Throwable t)
      {
         log.error("Failed to put the message in the delivery queue", t);
         return false;
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
