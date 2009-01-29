/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.jboss.messaging.ra;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.TopicConnection;
import javax.jms.XAConnection;
import javax.jms.XAQueueConnection;
import javax.jms.XATopicConnection;
import javax.naming.Reference;
import javax.resource.Referenceable;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ManagedConnectionFactory;

import org.jboss.messaging.core.logging.Logger;

/**
 * The connection factory
 * 
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision:  $
 */
public class JBMConnectionFactoryImpl implements JBMConnectionFactory, Referenceable
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMConnectionFactoryImpl.class);

   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The managed connection factory */
   private JBMManagedConnectionFactory mcf;

   /** The connection manager */
   private ConnectionManager cm;

   /** Naming reference */
   private Reference reference;

   /**
    * Constructor
    * @param mcf The managed connection factory
    * @param cm The connection manager
    */
   public JBMConnectionFactoryImpl(JBMManagedConnectionFactory mcf, ConnectionManager cm) 
   {
      if (trace)
         log.trace("constructor(" + mcf + ", " + cm + ")");

      this.mcf = mcf;

      if (cm == null)
      {
         // This is standalone usage, no appserver
         this.cm = new JBMConnectionManager();
         if (trace)
            log.trace("Created new ConnectionManager=" + cm);
      }
      else
         this.cm = cm;

      if (trace)
         log.trace("Using ManagedConnectionFactory=" + mcf + ", ConnectionManager=" + cm);
   }

   /**
    * Set the reference
    * @param reference The reference
    */
   public void setReference(Reference reference) 
   {
      if (trace)
         log.trace("setReference(" + reference + ")");

      this.reference = reference;
   }
    
   /**
    * Get the reference
    * @return The reference
    */
   public Reference getReference() 
   {
      if (trace)
         log.trace("getReference()");

      return reference;
   }
   
   /**
    * Create a queue connection
    * @return The connection
    * @exception JMSException Thrown if the operation fails
    */
   public QueueConnection createQueueConnection() throws JMSException 
   {
      if (trace)
         log.trace("createQueueConnection()");

      JBMSessionFactoryImpl s = new JBMSessionFactoryImpl(mcf, cm, QUEUE_CONNECTION);

      if (trace)
         log.trace("Created queue connection: " + s);
      
      return s;
   }
   
   /**
    * Create a queue connection
    * @param userName The user name
    * @param password The password
    * @return The connection
    * @exception JMSException Thrown if the operation fails
    */
   public QueueConnection createQueueConnection(String userName, String password) throws JMSException 
   {
      if (trace)
         log.trace("createQueueConnection(" + userName + ", ****)");

      JBMSessionFactoryImpl s = new JBMSessionFactoryImpl(mcf, cm, QUEUE_CONNECTION);
      s.setUserName(userName);
      s.setPassword(password);

      if (trace)
         log.trace("Created queue connection: " + s);
      
      return s;
   } 

   /**
    * Create a topic connection
    * @return The connection
    * @exception JMSException Thrown if the operation fails
    */
   public TopicConnection createTopicConnection() throws JMSException 
   {
      if (trace)
         log.trace("createTopicConnection()");

      JBMSessionFactoryImpl s = new JBMSessionFactoryImpl(mcf, cm, TOPIC_CONNECTION);

      if (trace)
         log.trace("Created topic connection: " + s);

      return s;
   }
   
   /**
    * Create a topic connection
    * @param userName The user name
    * @param password The password
    * @return The connection
    * @exception JMSException Thrown if the operation fails
    */
   public TopicConnection createTopicConnection(String userName, String password) throws JMSException 
   {
      if (trace)
         log.trace("createTopicConnection(" + userName + ", ****)");

      JBMSessionFactoryImpl s = new JBMSessionFactoryImpl(mcf, cm, TOPIC_CONNECTION);
      s.setUserName(userName);
      s.setPassword(password);
      
      if (trace)
         log.trace("Created topic connection: " + s);

      return s;
   }

   /**
    * Create a connection
    * @return The connection
    * @exception JMSException Thrown if the operation fails
    */
   public Connection createConnection() throws JMSException 
   {
      if (trace)
         log.trace("createConnection()");

      JBMSessionFactoryImpl s = new JBMSessionFactoryImpl(mcf, cm, CONNECTION);

      if (trace)
         log.trace("Created connection: " + s);

      return s;
   }

   /**
    * Create a connection
    * @param userName The user name
    * @param password The password
    * @return The connection
    * @exception JMSException Thrown if the operation fails
    */
   public Connection createConnection(String userName, String password) throws JMSException
   {
      if (trace)
         log.trace("createConnection(" + userName + ", ****)");

      JBMSessionFactoryImpl s = new JBMSessionFactoryImpl(mcf, cm, CONNECTION);
      s.setUserName(userName);
      s.setPassword(password);
      
      if (trace)
         log.trace("Created connection: " + s);

      return s;
   }  

   /**
    * Create a XA queue connection
    * @return The connection
    * @exception JMSException Thrown if the operation fails
    */
   public XAQueueConnection createXAQueueConnection() throws JMSException 
   {
      if (trace)
         log.trace("createXAQueueConnection()");

      JBMSessionFactoryImpl s = new JBMSessionFactoryImpl(mcf, cm, XA_QUEUE_CONNECTION);

      if (trace)
         log.trace("Created queue connection: " + s);
      
      return s;
   }
   
   /**
    * Create a XA  queue connection
    * @param userName The user name
    * @param password The password
    * @return The connection
    * @exception JMSException Thrown if the operation fails
    */
   public XAQueueConnection createXAQueueConnection(String userName, String password) throws JMSException 
   {
      if (trace)
         log.trace("createXAQueueConnection(" + userName + ", ****)");

      JBMSessionFactoryImpl s = new JBMSessionFactoryImpl(mcf, cm, XA_QUEUE_CONNECTION);
      s.setUserName(userName);
      s.setPassword(password);

      if (trace)
         log.trace("Created queue connection: " + s);
      
      return s;
   } 

   /**
    * Create a XA topic connection
    * @return The connection
    * @exception JMSException Thrown if the operation fails
    */
   public XATopicConnection createXATopicConnection() throws JMSException 
   {
      if (trace)
         log.trace("createXATopicConnection()");

      JBMSessionFactoryImpl s = new JBMSessionFactoryImpl(mcf, cm, XA_TOPIC_CONNECTION);

      if (trace)
         log.trace("Created topic connection: " + s);

      return s;
   }
   
   /**
    * Create a XA topic connection
    * @param userName The user name
    * @param password The password
    * @return The connection
    * @exception JMSException Thrown if the operation fails
    */
   public XATopicConnection createXATopicConnection(String userName, String password) throws JMSException 
   {
      if (trace)
         log.trace("createXATopicConnection(" + userName + ", ****)");

      JBMSessionFactoryImpl s = new JBMSessionFactoryImpl(mcf, cm, XA_TOPIC_CONNECTION);
      s.setUserName(userName);
      s.setPassword(password);
      
      if (trace)
         log.trace("Created topic connection: " + s);

      return s;
   }

   /**
    * Create a XA connection
    * @return The connection
    * @exception JMSException Thrown if the operation fails
    */
   public XAConnection createXAConnection() throws JMSException 
   {
      if (trace)
         log.trace("createXAConnection()");

      JBMSessionFactoryImpl s = new JBMSessionFactoryImpl(mcf, cm, XA_CONNECTION);

      if (trace)
         log.trace("Created connection: " + s);

      return s;
   }

   /**
    * Create a XA connection
    * @param userName The user name
    * @param password The password
    * @return The connection
    * @exception JMSException Thrown if the operation fails
    */
   public XAConnection createXAConnection(String userName, String password) throws JMSException
   {
      if (trace)
         log.trace("createXAConnection(" + userName + ", ****)");

      JBMSessionFactoryImpl s = new JBMSessionFactoryImpl(mcf, cm, XA_CONNECTION);
      s.setUserName(userName);
      s.setPassword(password);
      
      if (trace)
         log.trace("Created connection: " + s);

      return s;
   }  
}
