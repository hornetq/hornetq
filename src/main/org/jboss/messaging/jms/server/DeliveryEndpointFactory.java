/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.server;

import org.jboss.messaging.jms.message.JBossMessage;

/**
 * A factory for delivery endpoints
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public interface DeliveryEndpointFactory
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   DeliveryEndpoint getDeliveryEndpoint(MessageReference message);

   MessageReference getMessageReference(JBossMessage message);
}
