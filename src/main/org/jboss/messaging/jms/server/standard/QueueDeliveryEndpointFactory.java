/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.server.standard;

import org.jboss.messaging.jms.message.JBossMessage;
import org.jboss.messaging.jms.server.util.MessageList;
import org.jboss.messaging.jms.server.DeliveryEndpointFactory;
import org.jboss.messaging.jms.server.MessageBroker;
import org.jboss.messaging.jms.server.DeliveryEndpoint;
import org.jboss.messaging.jms.server.MessageReference;

/**
 * A queue delivery endpoint factory
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class QueueDeliveryEndpointFactory
   implements DeliveryEndpointFactory
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The message broker */
   private MessageBroker broker;

   /** The message list */
   private MessageList list;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public QueueDeliveryEndpointFactory(MessageBroker broker, MessageList list)
   {
      this.broker = broker;
      this.list = list;
   }

   // Public --------------------------------------------------------

   // DeliveryEndpointFactory implementation ------------------------

   public DeliveryEndpoint getDeliveryEndpoint(MessageReference message)
   {
      return new QueueDeliveryEndpoint(list);
   }

   public MessageReference getMessageReference(JBossMessage message)
   {
      return broker.getMessageReference(message);
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------
}
