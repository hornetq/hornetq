/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 * The implementation of a producer
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public interface ProducerDelegate 
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Notify about to close
    * 
    * @throws JMSException for any error
    */
   void closing() throws JMSException;

   /**
    * Close the delegate
    * 
    * @throws JMSException for any error
    */
   void close() throws JMSException;

   /**
    * Send a message
    * 
    * @param destination the destination
    * @param message the message
    * @param deliveryMode the delivery mode
    * @param priority the priority
    * @param timeToLive the time to live
    * @throws JMSException for any error
    */
   void send(Destination destination, Message message, int deliveryMode,
             int priority, long timeToLive)
      throws JMSException;

   // Inner Classes --------------------------------------------------
}
