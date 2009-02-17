/*
 * JORAM: Java(TM) Open Reliable Asynchronous Messaging
 * Copyright (C) 2002 INRIA
 * Contact: joram-team@objectweb.org
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 * 
 * Initial developer(s): Jeff Mesnil (jmesnil@gmail.com)
 * Contributor(s): ______________________________________.
 */

package org.objectweb.jtests.jms.admin;

import javax.naming.Context;
import javax.naming.NamingException;

/**
 * Simple Administration interface.
 * <br />
 * JMS Provider has to implement this 
 * simple interface to be able to use the test suite.
 */
public interface Admin
{

   /**
    * Returns the name of the JMS Provider.
    *
    * @return name of the JMS Provider
    */
   public String getName();

   /** 
    * Returns an <code>Context</code> for the JMS Provider.
    *
    * @return an <code>Context</code> for the JMS Provider.
    */
   public Context createContext() throws NamingException;

   /** 
    * Creates a <code>ConnectionFactory</code> and makes it available 
    *from JNDI with name <code>name</code>.
    *
    * @since JMS 1.1
    * @param name JNDI name of the <code>ConnectionFactory</code>
    */
   public void createConnectionFactory(String name);

   /** 
    * Creates a <code>QueueConnectionFactory</code> and makes it available 
    *from JNDI with name <code>name</code>.
    *
    * @param name JNDI name of the <code>QueueConnectionFactory</code>
    */
   public void createQueueConnectionFactory(String name);

   /** 
    * Creates a <code>TopicConnectionFactory</code> and makes it available 
    *from JNDI with name <code>name</code>.
    *
    * @param name JNDI name of the <code>TopicConnectionFactory</code>
    */
   public void createTopicConnectionFactory(String name);

   /** 
    * Creates a <code>Queue</code> and makes it available 
    *from JNDI with name <code>name</code>.
    *
    * @param name JNDI name of the <code>Queue</code>
    */
   public void createQueue(String name);

   /** 
    * Creates a <code>Topic</code> and makes it available 
    *from JNDI with name <code>name</code>.
    *
    * @param name JNDI name of the <code>Topic</code>
    */
   public void createTopic(String name);

   /** 
    * Removes the <code>Queue</code> of name <code>name</code> from JNDI and deletes it
    *
    * @param name JNDI name of the <code>Queue</code>
    */
   public void deleteQueue(String name);

   /** 
    * Removes the <code>Topic</code> of name <code>name</code> from JNDI and deletes it
    *
    * @param name JNDI name of the <code>Topic</code>
    */
   public void deleteTopic(String name);

   /** 
    * Removes the <code>ConnectionFactory</code> of name <code>name</code> from JNDI and deletes it
    *
    * @since JMS 1.1
    * @param name JNDI name of the <code>ConnectionFactory</code>
    */
   public void deleteConnectionFactory(String name);

   /** 
    * Removes the <code>QueueConnectionFactory</code> of name <code>name</code> from JNDI and deletes it
    *
    * @param name JNDI name of the <code>QueueConnectionFactory</code>
    */
   public void deleteQueueConnectionFactory(String name);

   /** 
    * Removes the <code>TopicConnectionFactory</code> of name <code>name</code> from JNDI and deletes it
    *
    * @param name JNDI name of the <code>TopicConnectionFactory</code>
    */
   public void deleteTopicConnectionFactory(String name);

   /**
    * Optional method to start the server embedded (instead of running an external server)
    */
   public void startEmbeddedServer() throws Exception;
   
   /**
    * Optional method to stop the server embedded (instead of running an external server)
    */
   public void stopEmbeddedServer() throws Exception;

   /**
    * Optional method for processing to be made after the Admin is instantiated and before
    * it is used to create the administrated objects
    */
   void start() throws Exception;

   /**
    * Optional method for processing to be made after the administrated objects have been cleaned up
    */
   void stop() throws Exception;

}
