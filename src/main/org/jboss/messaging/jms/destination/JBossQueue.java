/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.destination;

import org.jboss.messaging.jms.destination.JBossDestination;

import javax.jms.Queue;
import javax.jms.JMSException;

/**
 * A queue
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JBossQueue
   extends JBossDestination
   implements Queue
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Construct a new queue
    * 
    * @param name the name of the queue
    */
   public JBossQueue(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   // Queue implementation ------------------------------------------
   
   public String getQueueName()
      throws JMSException
   {
      return super.getName();
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
