/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client.facade;

import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.messaging.jms.client.ConnectionDelegate;
import org.jboss.messaging.jms.client.SessionDelegate;

import javax.jms.Connection;
import javax.jms.QueueConnection;
import javax.jms.TopicConnection;
import javax.jms.XAConnection;
import javax.jms.XAQueueConnection;
import javax.jms.XATopicConnection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Destination;
import javax.jms.ConnectionConsumer;
import javax.jms.ServerSessionPool;
import javax.jms.Topic;
import javax.jms.Session;
import javax.jms.ConnectionMetaData;
import javax.jms.Queue;
import javax.jms.QueueSession;
import javax.jms.TopicSession;
import javax.jms.XASession;
import javax.jms.XAQueueSession;
import javax.jms.XATopicSession;


/**
 * A connection.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public class JBossConnection 
   implements Connection,
              QueueConnection,
              TopicConnection,
              XAConnection,
              XAQueueConnection,
              XATopicConnection
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The connection delegate */
   private ConnectionDelegate delegate;

   /** The exception listener */
   private ExceptionListener listener;

   /** Are we an XAConnection */
   private boolean isXAConnection;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JBossConnection(ConnectionDelegate delegate, boolean isXAConnection)
   {
      this.delegate = delegate;
      this.isXAConnection = isXAConnection;
   }

   // Public --------------------------------------------------------

//   /**
//    * Retrieve the extension property names
//    */
//   public Iterator getJMSXPropertyNames()
//   {
//      return delegate.getJMSXPropertyNames();
//   }

   // Connection implementation -------------------------------------

	public void close() throws JMSException
	{
      delegate.closing();
      delegate.close();
	}

	public ConnectionConsumer createConnectionConsumer(Destination destination,
                                                      String messageSelector,
                                                      ServerSessionPool sessionPool,
                                                      int maxMessages) throws JMSException
	{
      throw new NotYetImplementedException();
	}

	public ConnectionConsumer createDurableConnectionConsumer(Topic topic,
                                                             String subscriptionName,
                                                             String messageSelector,
                                                             ServerSessionPool sessionPool,
                                                             int maxMessages) throws JMSException
	{
      throw new NotYetImplementedException();
	}

	public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException
	{
      if (transacted)
      {
         acknowledgeMode = Session.SESSION_TRANSACTED;
      }

      SessionDelegate sessionDelegate = delegate.createSession(false, transacted, acknowledgeMode);
      return new JBossSession(sessionDelegate, false, transacted, acknowledgeMode);
	}

	public String getClientID() throws JMSException
	{
      return delegate.getClientID();
	}

	public ExceptionListener getExceptionListener() throws JMSException
	{
      return listener;
	}

	public ConnectionMetaData getMetaData() throws JMSException
	{
      return new JBossConnectionMetaData(delegate);
	}

	public void setClientID(String clientID) throws JMSException
	{
      delegate.setClientID(clientID);
	}

	public void setExceptionListener(ExceptionListener listener) throws JMSException
	{
      delegate.setExceptionListener(listener);
      this.listener = listener;
	}

	public void start() throws JMSException
	{
      delegate.start();
	}

	public void stop() throws JMSException
	{
      delegate.stop();
	}

   // QueueConnection implementation --------------------------------

	public ConnectionConsumer createConnectionConsumer(Queue queue,
                                                      String messageSelector,
                                                      ServerSessionPool sessionPool,
                                                      int maxMessages) throws JMSException
	{
      return createConnectionConsumer((Destination)queue,
                                      messageSelector,
                                      sessionPool,
                                      maxMessages);
	}

	public QueueSession createQueueSession(boolean transacted, int acknowledgeMode)
         throws JMSException
	{
      return (QueueSession)createSession(transacted, acknowledgeMode);
	}

   // TopicConnection implementation --------------------------------

	public ConnectionConsumer createConnectionConsumer(Topic topic,
                                                      String messageSelector,
                                                      ServerSessionPool sessionPool,
                                                      int maxMessages) throws JMSException
	{
      return createConnectionConsumer((Destination)topic,
                                      messageSelector,
                                      sessionPool,
                                      maxMessages);
	}

	public TopicSession createTopicSession(boolean transacted, int acknowledgeMode)
         throws JMSException
	{
      return (TopicSession)createSession(transacted, acknowledgeMode);
	}

   // XAConnection implementation -----------------------------------

   public XASession createXASession() throws JMSException
   {
      if (isXAConnection == false)
      {
         throw new JMSException("Not an XA connection");
      }
      SessionDelegate sessionDelegate =
            delegate.createSession(true, true, Session.SESSION_TRANSACTED);
      return new JBossSession(sessionDelegate, true, true, Session.SESSION_TRANSACTED);
   }

   // XAQueueConnection implementation ------------------------------

   public XAQueueSession createXAQueueSession() throws JMSException
   {
      return (XAQueueSession)createXASession();
   }

   // XATopicConnection implementation ------------------------------

   public XATopicSession createXATopicSession() throws JMSException
   {
      return (XATopicSession)createXASession();
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------
}
