/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.standard;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.BrowserEndpointFactory;
import org.jboss.jms.server.DeliveryEndpointFactory;
import org.jboss.jms.server.MessageBroker;
import org.jboss.jms.server.MessageReference;
import org.jboss.jms.server.list.memory.MemoryMessageList;

/**
 * The standard message broker
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
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
