/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueConnectionFactory;
import javax.jms.XATopicConnection;
import javax.jms.XATopicConnectionFactory;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;

/**
 * A connection factory
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JBossConnectionFactory 
   implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory,
              XAConnectionFactory, XAQueueConnectionFactory, XATopicConnectionFactory,
              Referenceable
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The implementation delegate */
   ImplementationDelegate delegate;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Construct a new Connection factory
    * 
    * @param delegate the implementation
    */
   public JBossConnectionFactory(ImplementationDelegate delegate)
      throws JMSException
   {
      this.delegate = delegate;
   }

   // ConnectionFactory implementation ------------------------------

	public Connection createConnection()
      throws JMSException
	{
      return createConnection(null, null);
	}

	public Connection createConnection(String userName, String password)
      throws JMSException
	{
      ConnectionDelegate connection = delegate.createConnection(userName, password);
      return new JBossConnection(connection, false);
	}

   // QueueConnectionFactory implementation -------------------------

	public QueueConnection createQueueConnection()
      throws JMSException
	{
      return (QueueConnection) createConnection(null, null);
	}

	public QueueConnection createQueueConnection(String userName, String password)
      throws JMSException
	{
      return (QueueConnection) createConnection(userName, password);
	}

   // TopicConnectionFactory implementation -------------------------

	public TopicConnection createTopicConnection()
      throws JMSException
	{
      return (TopicConnection) createConnection(null, null);
	}

	public TopicConnection createTopicConnection(String userName, String password)
      throws JMSException
	{
      return (TopicConnection) createConnection(userName, password);
	}

   // XAConnectionFactory implementation ----------------------------

   public XAConnection createXAConnection() throws JMSException
   {
      return createXAConnection(null, null);
   }

   public XAConnection createXAConnection(String userName, String password) throws JMSException
   {
      ConnectionDelegate connection = delegate.createConnection(userName, password);
      return new JBossConnection(connection, true);
   }

   // XAQueueConnectionFactory implementation -----------------------

   public XAQueueConnection createXAQueueConnection() throws JMSException
   {
      return (XAQueueConnection) createXAConnection(null, null);
   }

   public XAQueueConnection createXAQueueConnection(String userName, String password) throws JMSException
   {
      return (XAQueueConnection) createXAConnection(userName, password);
   }

   // XATopicConnectionFactory implementation -----------------------

   public XATopicConnection createXATopicConnection() throws JMSException
   {
      return (XATopicConnection) createXAConnection(null, null);
   }

   public XATopicConnection createXATopicConnection(String userName, String password) throws JMSException
   {
      return (XATopicConnection) createXAConnection(userName, password);
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
