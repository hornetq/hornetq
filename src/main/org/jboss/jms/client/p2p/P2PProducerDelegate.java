/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.p2p;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.MessageImpl;
import org.jboss.jms.client.ProducerDelegate;

/**
 * The p2p producer
 * 
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class P2PProducerDelegate
   implements ProducerDelegate
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private P2PSessionDelegate session = null;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public P2PProducerDelegate(P2PSessionDelegate session, Destination destination)
      throws JMSException
   {
      this.session = session;
   }

   // Public --------------------------------------------------------

   // ProducerDelegate implementation -------------------------------

	public void close() throws JMSException
	{
	}

	public void closing() throws JMSException
	{
	}

   public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive)
      throws JMSException
   {
      MessageImpl clone = (MessageImpl) ((MessageImpl) message).clone();
      clone.setJMSDestination(destination);
      clone.setJMSDeliveryMode(deliveryMode);
      clone.setJMSPriority(priority);
      if (timeToLive != 0)
      {
          clone.setJMSExpiration(System.currentTimeMillis() + timeToLive);
      }
      clone.setReadOnly(true);
      this.session.send(clone);
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
