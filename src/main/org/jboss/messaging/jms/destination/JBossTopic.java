/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.destination;

import javax.jms.Topic;
import javax.jms.JMSException;

/**
 * A topic
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JBossTopic
   extends JBossDestination
   implements Topic
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Construct a new topic
    *
    * @param name the name of the topic
    */
   public JBossTopic(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   // Topic implementation ------------------------------------------
   
   public String getTopicName()
      throws JMSException
   {
      return super.getName();
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
