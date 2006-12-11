/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.jms.client;

import java.io.Serializable;

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

import org.jboss.aop.Advised;
import org.jboss.jms.client.container.JmsClientAspectXMLLoader;
import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.referenceable.SerializableObjectRefAddr;
import org.jboss.jms.server.endpoint.CreateConnectionResult;
import org.jboss.jms.util.ThreadContextClassLoaderChanger;
import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JBossConnectionFactory implements
               ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory,
               XAConnectionFactory, XAQueueConnectionFactory, XATopicConnectionFactory,
               Serializable/*, Referenceable http://jira.jboss.org/jira/browse/JBMESSAGING-395*/
{
   // Constants -----------------------------------------------------
   
   private final static long serialVersionUID = -2810634789345348326L;
   
   private static final Logger log = Logger.getLogger(JBossConnectionFactory.class);
   
   
   // Static --------------------------------------------------------
   
   private static boolean configLoaded;
   
   // Attributes ----------------------------------------------------
   
   protected ConnectionFactoryDelegate delegate;
   
   private boolean initialised;
   
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
      return createConnectionInternal(username, password, false, JBossConnection.TYPE_GENERIC_CONNECTION);
   }
   
   // QueueConnectionFactory implementation -------------------------
   
   public QueueConnection createQueueConnection() throws JMSException
   {
      return createQueueConnection(null, null);
   }
   
   public QueueConnection createQueueConnection(String username, String password) throws JMSException
   {
      return createConnectionInternal(username, password, false, JBossConnection.TYPE_QUEUE_CONNECTION);
   }
   
   // TopicConnectionFactory implementation -------------------------
   
   public TopicConnection createTopicConnection() throws JMSException
   {
      return createTopicConnection(null, null);
   }
   
   public TopicConnection createTopicConnection(String username, String password) throws JMSException
   {
      return createConnectionInternal(username, password, false, JBossConnection.TYPE_TOPIC_CONNECTION);
   }
   
   // XAConnectionFactory implementation ------------------------------
   
   public XAConnection createXAConnection() throws JMSException
   {
      return createXAConnection(null, null);
   }
   
   public XAConnection createXAConnection(String username, String password) throws JMSException
   {
      return createConnectionInternal(username, password, true, JBossConnection.TYPE_GENERIC_CONNECTION);   
   }
   
   // XAQueueConnectionFactory implementation -------------------------
   
   public XAQueueConnection createXAQueueConnection() throws JMSException
   {
      return createXAQueueConnection(null, null);
   }
   
   public XAQueueConnection createXAQueueConnection(String username, String password) throws JMSException
   {
      return createConnectionInternal(username, password, true, JBossConnection.TYPE_QUEUE_CONNECTION);   
   }
   
   // XATopicConnectionFactory implementation -------------------------
   
   public XATopicConnection createXATopicConnection() throws JMSException
   {
      return createXATopicConnection(null, null);
   }
   
   public XATopicConnection createXATopicConnection(String username, String password) throws JMSException
   {
      return createConnectionInternal(username, password, true, JBossConnection.TYPE_TOPIC_CONNECTION);   
   }
   
   // Referenceable implementation ---------------------------------------
   
   public Reference getReference() throws NamingException
   {
      return new Reference("org.jboss.jms.client.JBossConnectionFactory",
               new SerializableObjectRefAddr("JBM-CF", this),
               "org.jboss.jms.referenceable.ConnectionFactoryObjectFactory",
               null);
   }
   
   // Public --------------------------------------------------------
   
   public String toString()
   {
      return "JBossConnectionFactory->" + delegate;
   }
   
   public ConnectionFactoryDelegate getDelegate()
   {
      return delegate;
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected JBossConnection createConnectionInternal(String username, String password,
            boolean isXA, int type)
      throws JMSException
   {
      
      ThreadContextClassLoaderChanger tccc = new ThreadContextClassLoaderChanger();
      
      try
      {
         tccc.set(getClass().getClassLoader());
         
         ensureAOPConfigLoaded((ClientConnectionFactoryDelegate)delegate);
         initDelegate();
         
         // The version used by the connection is the minimum of the server version for the
         // connection factory and the client code version
         
         CreateConnectionResult res = delegate.createConnectionDelegate(username, password, -1);        
         
         return new JBossConnection(res.getDelegate(), type);
      }
      finally
      {
         tccc.restore();
      }
   }
   
   protected synchronized void initDelegate()
   {
      if (!initialised)
      {
         ((ClientConnectionFactoryDelegate)delegate).init();
         initialised = true;
      }         
   }
   
   protected void ensureAOPConfigLoaded(ClientConnectionFactoryDelegate delegate)
   {
      try
      {
         synchronized (JBossConnectionFactory.class)
         {
            if (!configLoaded)
            {
               // Load the client side aspect stack configuration from the server and apply it
               
               delegate.init();
               
               byte[] clientAOPConfig = delegate.getClientAOPConfig();
               
               // Remove interceptor since we don't want it on the front of the stack
               ((Advised)delegate)._getInstanceAdvisor().removeInterceptor(delegate.getName());
               
               JmsClientAspectXMLLoader loader = new JmsClientAspectXMLLoader();
               
               loader.deployXML(clientAOPConfig);
               
               configLoaded = true;               
            }
         }   
      }
      catch(Exception e)
      {
         // Need to log message since no guarantee that client will log it
         final String msg = "Failed to config client side AOP";
         log.error(msg, e);
         throw new RuntimeException(msg, e);
      }
   }
   
   // Private -------------------------------------------------------
      
   // Inner classes -------------------------------------------------
}
