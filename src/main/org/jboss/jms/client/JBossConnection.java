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
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.JMSException;
import javax.jms.ConnectionMetaData;
import javax.jms.ExceptionListener;
import javax.jms.ConnectionConsumer;
import javax.jms.Destination;
import javax.jms.ServerSessionPool;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.XAConnection;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicConnection;
import javax.jms.XATopicSession;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JBossConnection implements
    Connection, QueueConnection, TopicConnection,
    XAConnection, XAQueueConnection, XATopicConnection
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ConnectionDelegate delegate;
   private boolean isXAConnection;
   private ExceptionListener exceptionListener;

   // Constructors --------------------------------------------------

   public JBossConnection(ConnectionDelegate delegate, boolean isXAConnection)
   {
      this.delegate = delegate;
      this.isXAConnection = isXAConnection;
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
      return new JBossConnectionMetaData();
   }

   public ExceptionListener getExceptionListener() throws JMSException
   {
      return exceptionListener;
   }

   public void setExceptionListener(ExceptionListener listener) throws JMSException
   {
      this.exceptionListener = listener;
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
   
   // QueueConnection implementation ---------------------------------

   public QueueSession createQueueSession(boolean transacted,
                                          int acknowledgeMode) throws JMSException
   {
       return (QueueSession)createSession(transacted, acknowledgeMode);
   }
   
   public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector,
                                                      ServerSessionPool sessionPool,
                                                      int maxMessages) throws JMSException
    {
       throw new NotYetImplementedException();
    }
   
   // TopicConnection implementation ---------------------------------

   public TopicSession createTopicSession(boolean transacted,
                                          int acknowledgeMode) throws JMSException
   {
       return (TopicSession)createSession(transacted, acknowledgeMode);
   }
   
   public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector,
                                                      ServerSessionPool sessionPool,
                                                      int maxMessages) throws JMSException
   {
       throw new NotYetImplementedException();
   }
   
   // XAConnection implementation -------------------------------------

   public XASession createXASession() throws JMSException
   {
       if (!isXAConnection) throw new JMSException("Not an XA connection");       
       return (XASession)createSession(true, Session.SESSION_TRANSACTED);
   }
   
   // XAQueueConnection implementation ---------------------------------

   public XAQueueSession createXAQueueSession() throws JMSException
   {
       return (XAQueueSession)createXASession();
   }
   
   // XATopicConnection implementation ---------------------------------

   public XATopicSession createXATopicSession() throws JMSException
   {
       return (XATopicSession)createXASession();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
