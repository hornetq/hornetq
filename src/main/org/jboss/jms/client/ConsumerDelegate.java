/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

/**
 * The implementation of a consumer
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public interface ConsumerDelegate 
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
    * Receive a message
    * 
    * @param timeout the timeout
    * @return the message
    * @throws JMSException for any error
    */
   Message receive(long timeout) throws JMSException;

   /**
    *
    * Set the message listener
    * @param the new message listener
    * @throws JMSException for any error 
    */
   void setMessageListener(MessageListener listener) throws JMSException;

   // Inner Classes --------------------------------------------------
}
