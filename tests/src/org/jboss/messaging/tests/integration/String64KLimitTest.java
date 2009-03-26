/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration;

import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * 
 * There is a bug in JDK1.3, 1.4 whereby writeUTF fails if more than 64K bytes are written
 * we need to work with all size of strings
 * 
 * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4806007
 * http://jira.jboss.com/jira/browse/JBAS-2641
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision: 6016 $
 *
 * $Id: String64KLimitTest.java 6016 2009-03-06 10:40:09Z jmesnil $
 */
public class String64KLimitTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingServiceImpl service;

   private ClientSession session;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   protected String genString(int len)
   {
      char[] chars = new char[len];
      for (int i = 0; i < len; i++)
      {
         chars[i] = (char)(65 + i % 26);
      }
      return new String(chars);
   }

   public void test64KLimitWithWriteString() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      session.createQueue(address, queue, false);

      ClientProducer producer = session.createProducer(address);
      ClientConsumer consumer = session.createConsumer(queue);
      session.start();

      String s1 = genString(16 * 1024);

      String s2 = genString(32 * 1024);

      String s3 = genString(64 * 1024);

      String s4 = genString(10 * 64 * 1024);

      ClientMessage tm1 = session.createClientMessage(false);
      tm1.getBody().writeString(s1);

      ClientMessage tm2 = session.createClientMessage(false);
      tm2.getBody().writeString(s2);

      ClientMessage tm3 = session.createClientMessage(false);
      tm3.getBody().writeString(s3);

      ClientMessage tm4 = session.createClientMessage(false);
      tm4.getBody().writeString(s4);

      producer.send(tm1);

      producer.send(tm2);

      producer.send(tm3);

      producer.send(tm4);

      ClientMessage rm1 = consumer.receive(1000);

      assertNotNull(rm1);

      ClientMessage rm2 = consumer.receive(1000);

      assertNotNull(rm2);

      ClientMessage rm3 = consumer.receive(1000);

      assertNotNull(rm3);

      ClientMessage rm4 = consumer.receive(1000);

      assertNotNull(rm4);

      assertEquals(s1, rm1.getBody().readString());

      assertEquals(s2, rm2.getBody().readString());

      assertEquals(s3, rm3.getBody().readString());

      assertEquals(s4, rm4.getBody().readString());
   }

   public void test64KLimitWithWriteUTF() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();
      
      session.createQueue(address, queue, false);

      ClientProducer producer = session.createProducer(address);
      ClientConsumer consumer = session.createConsumer(queue);

      session.start();
      
      String s1 = genString(16 * 1024);

      String s2 = genString(32 * 1024);

      String s3 = genString(64 * 1024);

      String s4 = genString(10 * 64 * 1024);

      ClientMessage tm1 = session.createClientMessage(false);
      tm1.getBody().writeUTF(s1);

      ClientMessage tm2 = session.createClientMessage(false);
      tm2.getBody().writeUTF(s2);

      try
      {
         ClientMessage tm3 = session.createClientMessage(false);
         tm3.getBody().writeUTF(s3);
         fail("can not write UTF string bigger than 64K");
      }
      catch (Exception e)
      {
      }

      try
      {
         ClientMessage tm4 = session.createClientMessage(false);
         tm4.getBody().writeUTF(s4);
         fail("can not write UTF string bigger than 64K");
      }
      catch (Exception e)
      {
      }

      producer.send(tm1);
      producer.send(tm2);

      ClientMessage rm1 = consumer.receive(1000);

      assertNotNull(rm1);

      ClientMessage rm2 = consumer.receive(1000);

      assertNotNull(rm2);
     
      assertEquals(s1, rm1.getBody().readUTF());
      assertEquals(s2, rm2.getBody().readUTF());
   }

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration config = new ConfigurationImpl();
      config.setSecurityEnabled(false);
      service = Messaging.newNullStorageMessagingService(config);
      service.start();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      session.close();

      service.stop();

      super.tearDown();
   }
}
