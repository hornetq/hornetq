/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client;

import org.jboss.messaging.jms.destination.JBossTemporaryDestination;

import javax.jms.JMSException;
import javax.jms.ExceptionListener;
import java.util.Enumeration;


/**
 * The implementation of a connection.
 *
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface ConnectionDelegate extends Lifecycle
{
   /**
    * Create a session delegate.
    *
    * @param isXA
    * @param transacted whether the session is transacted.
    * @param acknowledgeMode the acknowledgement mode.
    * @return the session delegate.
    * @throws JMSException for any error.
    */
   SessionDelegate createSession(boolean isXA, boolean transacted, int acknowledgeMode)
         throws JMSException;

   /**
    * Set the client id.
    *
    * @param id the client id.
    * @throws JMSException for any error.
    */
   void setClientID(String id) throws JMSException;

   /**
    * Retrieve the client id.
    *
    * @return the client id.
    * @throws JMSException for any error.
    */
   String getClientID() throws JMSException;

   /**
    * Set the exception listener.
    *
    * @param listener the new exception listener.
    * @throws JMSException for any error.
    */
   void setExceptionListener(ExceptionListener listener) throws JMSException;

   /**
    * Start the connection.
    *
    * @throws JMSException for any error.
    */
   void start() throws JMSException;

   /**
    * Stop the connection.
    *
    * @throws JMSException for any error.
    */
   void stop() throws JMSException;

   /**
    * Retrieve the extension property names
    *
    * @return an enumeration of extension properties
    * @throws JMSException for any error
    */
   Enumeration getJMSXPropertyNames() throws JMSException;

   /**
    * Delete the temporary destination
    *
    * @param destination the destination to delete
    */
   void deleteTempDestination(JBossTemporaryDestination destination);

}
