/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.messaging.util.NotYetImplementedException;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.JMSException;
import javax.jms.ConnectionMetaData;
import javax.jms.ExceptionListener;
import javax.jms.ConnectionConsumer;
import javax.jms.Destination;
import javax.jms.ServerSessionPool;
import javax.jms.Topic;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JBossConnection implements Connection
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ConnectionDelegate delegate;

   // Constructors --------------------------------------------------

   public JBossConnection(ConnectionDelegate delegate)
   {
      this.delegate = delegate;
   }

   // Connection implementation -------------------------------------

   public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException
   {
      SessionDelegate sessionDelegate = delegate.createSessionDelegate(transacted, acknowledgeMode);
      return new JBossSession(sessionDelegate);
   }

   public String getClientID() throws JMSException
   {
      return delegate.getClientID();
   }

   public void setClientID(String clientID) throws JMSException
   {
      if (delegate.getClientID() != null)
      {
         throw new IllegalStateException("An administratively configured connection identifier " +
                                         "already exists.");
      }
      delegate.setClientID(clientID);
   }

   public ConnectionMetaData getMetaData() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public ExceptionListener getExceptionListener() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setExceptionListener(ExceptionListener listener) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void start() throws JMSException
   {
      delegate.start();
   }

   public void stop() throws JMSException
   {
      delegate.stop();
   }

   public void close() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public ConnectionConsumer createConnectionConsumer(
         Destination destination,
         String messageSelector,
         ServerSessionPool sessionPool,
         int maxMessages)
         throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public ConnectionConsumer createDurableConnectionConsumer(
         Topic topic,
         String subscriptionName,
         String messageSelector,
         ServerSessionPool sessionPool,
         int maxMessages)
         throws JMSException
   {
      throw new NotYetImplementedException();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
