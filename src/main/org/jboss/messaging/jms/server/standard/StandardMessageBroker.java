/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.server.standard;

import org.jboss.messaging.jms.destination.JBossDestination;
import org.jboss.messaging.jms.message.JBossMessage;
import org.jboss.messaging.jms.server.util.MemoryMessageList;
import org.jboss.messaging.jms.server.MessageBroker;
import org.jboss.messaging.jms.server.BrowserEndpointFactory;
import org.jboss.messaging.jms.server.DeliveryEndpointFactory;
import org.jboss.messaging.jms.server.MessageReference;

/**
 * The standard message broker
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public class StandardMessageBroker
   implements MessageBroker
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The message list */
   private MemoryMessageList list = new MemoryMessageList();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // MessageBroker implementation ----------------------------------

   public BrowserEndpointFactory getBrowserEndpointFactory(JBossDestination destination, String selector)
   {
      return new QueueBrowserEndpointFactory(list, selector);
   }

   public DeliveryEndpointFactory getDeliveryEndpointFactory(JBossDestination destination)
   {
      return new QueueDeliveryEndpointFactory(this, list);
   }

   public MessageReference getMessageReference(JBossMessage message)
   {
      return new StandardMessageReference(message);
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
