/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.standard;

import org.jboss.jms.server.DeliveryEndpoint;
import org.jboss.jms.server.MessageReference;
import org.jboss.jms.server.list.MessageList;

/**
 * A queue delivery endpoint
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class QueueDeliveryEndpoint
   implements DeliveryEndpoint
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The message list */
   private MessageList list;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public QueueDeliveryEndpoint(MessageList list)
   {
      this.list = list;
   }

   // Public --------------------------------------------------------

   // DeliveryEndpoint implementation -------------------------------

   public void deliver(MessageReference message)
      throws Exception
   {
      list.add(message);
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
