/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client.facade;

import org.jboss.messaging.jms.client.facade.JBossConnection;
import org.jboss.messaging.jms.client.ConnectionDelegateFactory;
import org.jboss.messaging.jms.client.ConnectionDelegate;

import javax.jms.ConnectionFactory;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnectionFactory;
import javax.jms.XAConnectionFactory;
import javax.jms.XAQueueConnectionFactory;
import javax.jms.XATopicConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Connection;
import javax.jms.QueueConnection;
import javax.jms.TopicConnection;
import javax.jms.XAConnection;
import javax.jms.XAQueueConnection;
import javax.jms.XATopicConnection;
import javax.naming.Referenceable;
import javax.naming.NamingException;
import javax.naming.Reference;

/**
 * A connection factory.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public class JBossConnectionFactory 
   implements ConnectionFactory,
              QueueConnectionFactory,
              TopicConnectionFactory,
              XAConnectionFactory,
              XAQueueConnectionFactory,
              XATopicConnectionFactory,
              Referenceable
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The delegate */
   ConnectionDelegateFactory delegate;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Construct a new Connection Factory.
    * 
    * @param delegate the implementation
    */
   public JBossConnectionFactory(ConnectionDelegateFactory delegate)
   {
      this.delegate = delegate;
   }

   // ConnectionFactory implementation ------------------------------

	public Connection createConnection() throws JMSException
	{
      return createConnection(null, null);
	}

	public Connection createConnection(String userName, String password) throws JMSException
	{
      ConnectionDelegate connectionDelegate = delegate.createConnectionDelegate(userName, password);
      return new JBossConnection(connectionDelegate, false);
	}

   // QueueConnectionFactory implementation -------------------------

	public QueueConnection createQueueConnection() throws JMSException
	{
      return (QueueConnection)createConnection(null, null);
	}

	public QueueConnection createQueueConnection(String userName, String password)
         throws JMSException
	{
      return (QueueConnection)createConnection(userName, password);
	}

   // TopicConnectionFactory implementation -------------------------

	public TopicConnection createTopicConnection() throws JMSException
	{
      return (TopicConnection)createConnection(null, null);
	}

	public TopicConnection createTopicConnection(String userName, String password)
      throws JMSException
	{
      return (TopicConnection)createConnection(userName, password);
	}

   // XAConnectionFactory implementation ----------------------------

   public XAConnection createXAConnection() throws JMSException
   {
      return createXAConnection(null, null);
   }

   public XAConnection createXAConnection(String userName, String password) throws JMSException
   {
      ConnectionDelegate connectionDelegate = delegate.createConnectionDelegate(userName, password);
      return new JBossConnection(connectionDelegate, true);
   }

   // XAQueueConnectionFactory implementation -----------------------

   public XAQueueConnection createXAQueueConnection() throws JMSException
   {
      return (XAQueueConnection)createXAConnection(null, null);
   }

   public XAQueueConnection createXAQueueConnection(String userName, String password)
         throws JMSException
   {
      return (XAQueueConnection)createXAConnection(userName, password);
   }

   // XATopicConnectionFactory implementation -----------------------

   public XATopicConnection createXATopicConnection() throws JMSException
   {
      return (XATopicConnection)createXAConnection(null, null);
   }

   public XATopicConnection createXATopicConnection(String userName, String password)
         throws JMSException
   {
      return (XATopicConnection)createXAConnection(userName, password);
   }

   // Referenceable implementation------------------------------------

   public Reference getReference() throws NamingException
   {
      return delegate.getReference();
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
