/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration;

import static org.hornetq.tests.util.RandomUtil.randomSimpleString;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

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

   private HornetQServer server;

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
      tm1.getBodyBuffer().writeString(s1);

      ClientMessage tm2 = session.createClientMessage(false);
      tm2.getBodyBuffer().writeString(s2);

      ClientMessage tm3 = session.createClientMessage(false);
      tm3.getBodyBuffer().writeString(s3);

      ClientMessage tm4 = session.createClientMessage(false);
      tm4.getBodyBuffer().writeString(s4);

      producer.send(tm1);

      producer.send(tm2);

      producer.send(tm3);

      producer.send(tm4);

      ClientMessage rm1 = consumer.receive(1000);

      assertNotNull(rm1);

      assertEquals(s1, rm1.getBodyBuffer().readString());

      ClientMessage rm2 = consumer.receive(1000);

      assertNotNull(rm2);

      assertEquals(s2, rm2.getBodyBuffer().readString());

      ClientMessage rm3 = consumer.receive(1000);

      assertEquals(s3, rm3.getBodyBuffer().readString());

      assertNotNull(rm3);

      ClientMessage rm4 = consumer.receive(1000);

      assertEquals(s4, rm4.getBodyBuffer().readString());

      assertNotNull(rm4);
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
      tm1.getBodyBuffer().writeUTF(s1);

      ClientMessage tm2 = session.createClientMessage(false);
      tm2.getBodyBuffer().writeUTF(s2);

      try
      {
         ClientMessage tm3 = session.createClientMessage(false);
         tm3.getBodyBuffer().writeUTF(s3);
         fail("can not write UTF string bigger than 64K");
      }
      catch (Exception e)
      {
      }

      try
      {
         ClientMessage tm4 = session.createClientMessage(false);
         tm4.getBodyBuffer().writeUTF(s4);
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

      assertEquals(s1, rm1.getBodyBuffer().readUTF());
      assertEquals(s2, rm2.getBodyBuffer().readUTF());
   }

   // Protected -----------------------------------------------------

   private ClientSessionFactory sf;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration config = new ConfigurationImpl();
      config.setSecurityEnabled(false);
      config.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      server = HornetQ.newHornetQServer(config, false);
      server.start();

      sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));      
      session = sf.createSession();
   }

   @Override
   protected void tearDown() throws Exception
   {
      session.close();

      sf.close();

      server.stop();
      
      server = null;
      sf = null;
      session = null;

      super.tearDown();
   }
}
