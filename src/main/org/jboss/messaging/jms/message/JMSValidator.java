/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.message;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;

/**
 * Standard validation 
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JMSValidator
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   /**
    * Validate the delivery mode
    * 
    * @param deliveryMode the delivery mode to validate
    * @throws JMSException for any error 
    */
   public static void validateDeliveryMode(int deliveryMode)
      throws JMSException
   {
      if (deliveryMode != DeliveryMode.NON_PERSISTENT &&
          deliveryMode != DeliveryMode.PERSISTENT)
         throw new JMSException("Invalid delivery mode " + deliveryMode);
   }

   /**
    * Validate the priority
    * 
    * @param priority the priority to validate
    * @throws JMSException for any error 
    */
   public static void validatePriority(int priority)
      throws JMSException
   {
      if (priority < 0 || priority > 9)
         throw new JMSException("Invalid priority " + priority);
   }

   /**
    * Validate the time to live
    * 
    * @param timeToLive the ttl to validate
    * @throws JMSException for any error 
    */
   public static void validateTimeToLive(long timeToLive)
      throws JMSException
   {
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------
}
