/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.message;

import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.messaging.jms.client.SessionDelegate;

/**
 * A jboss message
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public interface JBossMessage
   extends Message, Cloneable
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   Object clone() throws CloneNotSupportedException;

   /**
    * Retrieve the session for this message
    * 
    * @return the session delegate
    * @throws JMSException for any error
    */
   SessionDelegate getSessionDelegate() throws JMSException;

   /**
    * Generate a message id for the message
    */
   void generateMessageID() throws JMSException;

   /**
    * Generate a timestamp for the message
    */
   void generateTimestamp() throws JMSException;

   /**
    * Change the message to read only
    */
   void makeReadOnly() throws JMSException;

   // Inner Classes --------------------------------------------------

}
