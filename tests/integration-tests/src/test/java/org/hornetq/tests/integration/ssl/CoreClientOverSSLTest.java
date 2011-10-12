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

package org.hornetq.tests.integration.ssl;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision: 3716 $</tt>
 * 
 */
public class CoreClientOverSSLTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   public static final String MESSAGE_TEXT_FROM_CLIENT = "CoreClientOverSSLTest from client";

   public static final SimpleString QUEUE = new SimpleString("QueueOverSSL");

   public static final int SSL_PORT = 5402;

   // Static --------------------------------------------------------

   private static final Logger log = Logger.getLogger(CoreClientOverSSLTest.class);

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSSL() throws Exception
   {
      String text = RandomUtil.randomString();

      TransportConfiguration tc = new TransportConfiguration(NettyConnectorFactory.class.getName());
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PATH);
      tc.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PASSWORD);

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(tc);
      ClientSessionFactory sf = locator.createSessionFactory();
      ClientSession session = sf.createSession(false, true, true);
      session.createQueue(CoreClientOverSSLTest.QUEUE, CoreClientOverSSLTest.QUEUE, false);
      ClientProducer producer = session.createProducer(CoreClientOverSSLTest.QUEUE);

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = session.createConsumer(CoreClientOverSSLTest.QUEUE);
      session.start();

      Message m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
      locator.close();
   }

   public void testSSLWithIncorrectKeyStorePassword() throws Exception
   {
      TransportConfiguration tc = new TransportConfiguration(NettyConnectorFactory.class.getName());
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PATH);
      tc.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "invalid password");

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(tc);
      try
      {
         ClientSessionFactory sf = locator.createSessionFactory();
         Assert.fail();
      }
      catch (HornetQException e)
      {
         Assert.assertEquals(HornetQException.NOT_CONNECTED, e.getCode());
      }
      finally
      {
         locator.close();
      }
   }

   // see https://jira.jboss.org/jira/browse/HORNETQ-234
   public void testPlainConnectionToSSLEndpoint() throws Exception
   {
      TransportConfiguration tc = new TransportConfiguration(NettyConnectorFactory.class.getName());
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, false);

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(tc);
      locator.setCallTimeout(2000);
      ClientSessionFactory sf = locator.createSessionFactory();
      try
      {
         sf.createSession(false, true, true);
         Assert.fail();
      }
      catch (HornetQException e)
      {
         Assert.assertEquals(HornetQException.CONNECTION_TIMEDOUT, e.getCode());
      }
      finally
      {
         locator.close();
      }
   }

   // Package protected ---------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      ConfigurationImpl config = createBasicConfig();
      config.setSecurityEnabled(false);
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      config.getAcceptorConfigurations().add(new TransportConfiguration(NettyAcceptorFactory.class.getName(), params));
      server = HornetQServers.newHornetQServer(config, false);
      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server.stop();

      super.tearDown();
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
