/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.destination;

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
    * @param the name of the topic
    * @throws JMSException for any error
    */
   public JBossTopic(String name)
      throws JMSException
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
