/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.message;

import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.client.SessionDelegate;

/**
 * A jboss message
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public interface JBossMessage
   extends Message
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

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

   // Inner Classes --------------------------------------------------

}
