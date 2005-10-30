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
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
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

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   protected JBossConnection createConnectionInternal(String username, String password, boolean isXA, int type)
      throws JMSException
   {
      ConnectionDelegate cd = delegate.createConnectionDelegate(username, password);
      return new JBossConnection(cd, type);
   }
   
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
