/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import java.util.Enumeration;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

/**
 * The implementation of a connection
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public interface ConnectionDelegate 
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Notify about closing
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
    * Create a session
    * 
    * @param transacted whether the session is transacted
    * @param the acknowledgement mode
    * @return the session
    * @throws JMSException for any error
    */
   SessionDelegate createSession(boolean isXA, boolean transacted, int acknowledgeMode) throws JMSException;

   /**
    * Retrieve the extension property names
    * 
    * @return an enumeration of extension properties
    * @throws JMSException for any error
    */
   Enumeration getJMSXPropertyNames() throws JMSException;

   /**
    * Retrieve the client id
    * 
    * @return the client id
    * @throws JMSException for any error
    */
   String getClientID() throws JMSException;

   /**
    * Set the client id
    * 
    * @param id the client id
    * @throws JMSException for any error
    */
   void setClientID(String id) throws JMSException;

   /**
    * Set the exception listener
    * 
    * @param the new exception listener
    * @throws JMSException for any error 
    */
   void setExceptionListener(ExceptionListener listener) throws JMSException;

   /**
    * Start the connection
    * 
    * @throws JMSException for any error 
    */
   void start() throws JMSException;

   /**
    * Stop the connection
    * 
    * @throws JMSException for any error 
    */
   void stop() throws JMSException;

   // Inner Classes --------------------------------------------------
}
