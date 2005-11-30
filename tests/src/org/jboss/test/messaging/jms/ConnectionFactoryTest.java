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
package org.jboss.test.messaging.jms;

import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.naming.InitialContext;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFactoryTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;

   // Constructors --------------------------------------------------

   public ConnectionFactoryTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.init("all");
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
   }

   public void tearDown() throws Exception
   {
      //ServerManagement.deInit();
      super.tearDown();
   }

   /* Test that ConnectionFactory can be cast to QueueConnectionFactory
    * and QueueConnection can be created
    */
   public void testQueueConnectionFactory() throws Exception
   {
      QueueConnectionFactory qcf =
         (QueueConnectionFactory)initialContext.lookup("/ConnectionFactory");
      QueueConnection qc = qcf.createQueueConnection();
      qc.close();
   }
   
   /* Test that ConnectionFactory can be cast to TopicConnectionFactory
    * and TopicConnection can be created
    */
   public void testTopicConnectionFactory() throws Exception
   {
      TopicConnectionFactory qcf =
         (TopicConnectionFactory)initialContext.lookup("/ConnectionFactory");
      TopicConnection tc = qcf.createTopicConnection();
      tc.close();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
