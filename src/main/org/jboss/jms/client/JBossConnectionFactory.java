/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.delegate.ConnectionDelegate;

import javax.jms.ConnectionFactory;
import javax.jms.Connection;
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

import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JBossConnectionFactory implements
    ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory,
    XAConnectionFactory, XAQueueConnectionFactory, XATopicConnectionFactory,
    Serializable
{
   // Constants -----------------------------------------------------

   private final static long serialVersionUID = -2810634789345348326L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ConnectionFactoryDelegate delegate;

   // Constructors --------------------------------------------------

   public JBossConnectionFactory(ConnectionFactoryDelegate delegate)
   {
      this.delegate = delegate;
   }

   // ConnectionFactory implementation ------------------------------

   public Connection createConnection() throws JMSException
   {
      return createConnection(null, null);
   }

   public Connection createConnection(String username, String password) throws JMSException
   {
      ConnectionDelegate cd = delegate.createConnectionDelegate(username, password);
      return new JBossConnection(cd, false);
   }
   
   // QueueConnectionFactory implementation -------------------------

   public QueueConnection createQueueConnection() throws JMSException
   {
       return (QueueConnection)createConnection(null, null);
   }

   public QueueConnection createQueueConnection(String username, String password) throws JMSException
   {
       return (QueueConnection)createConnection(username, password);
   }
   
   // TopicConnectionFactory implementation -------------------------

   public TopicConnection createTopicConnection() throws JMSException
   {
       return (TopicConnection)createConnection(null, null);
   }

   public TopicConnection createTopicConnection(String username, String password) throws JMSException
   {
       return (TopicConnection)createConnection(username, password);
   }
   
   // XAConnectionFactory implementation ------------------------------

   public XAConnection createXAConnection() throws JMSException
   {
      return createXAConnection(null, null);
   }

   public XAConnection createXAConnection(String username, String password) throws JMSException
   {
      ConnectionDelegate cd = delegate.createConnectionDelegate(username, password);
      return new JBossConnection(cd, true);
   }
   
   // XAQueueConnectionFactory implementation -------------------------

   public XAQueueConnection createXAQueueConnection() throws JMSException
   {
       return (XAQueueConnection)createXAConnection(null, null);
   }

   public XAQueueConnection createXAQueueConnection(String username, String password) throws JMSException
   {
       return (XAQueueConnection)createXAConnection(username, password);
   }
   
   // XATopicConnectionFactory implementation -------------------------
   
   public XATopicConnection createXATopicConnection() throws JMSException
   {
       return (XATopicConnection)createXAConnection(null, null);
   }

   public XATopicConnection createXATopicConnection(String username, String password) throws JMSException
   {
       return (XATopicConnection)createXAConnection(username, password);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
