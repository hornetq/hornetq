/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;

/**
 * A message broker
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public interface MessageBroker
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   BrowserEndpointFactory getBrowserEndpointFactory(JBossDestination destination, String selector);

   DeliveryEndpointFactory getDeliveryEndpointFactory(JBossDestination destination);

   MessageReference getMessageReference(JBossMessage message);
}
