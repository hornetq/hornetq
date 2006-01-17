/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.server.plugin;

import java.util.Set;
import java.util.Collections;

import javax.jms.JMSException;

import org.jboss.jms.server.plugin.DurableSubscriptionStoreSupport;
/**
 * Simple in-memory subscription support.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.com">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class InMemorySubscriptionStore extends DurableSubscriptionStoreSupport
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // DurableSubscriptionStoreSupport implementation ----------------

   public String getPreConfiguredClientID(String username) throws JMSException
   {
      return null;
   }

   public Set loadDurableSubscriptionsForTopic(String topicName) throws JMSException
   {
      return Collections.EMPTY_SET;
   }

   public Object getInstance()
   {
      return null;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
