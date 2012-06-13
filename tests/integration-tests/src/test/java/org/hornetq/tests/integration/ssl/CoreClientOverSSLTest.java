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
import org.hornetq.api.core.NotConnectedException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision: 3716 $</tt>
 *
 */
public class CoreClientOverSSLTest extends ServiceTestBase
{
   // Constants -----------------------------------------------------

   public static final String MESSAGE_TEXT_FROM_CLIENT = CoreClientOverSSLTest.class.getSimpleName() + " from client";

   public static final SimpleString QUEUE = new SimpleString("QueueOverSSL");

   public static final int SSL_PORT = 5402;

   private HornetQServer server;

   private TransportConfiguration tc;

   public void testSSL() throws Exception
   {
      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PATH);
      tc.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PASSWORD);

      ServerLocator locator = addServerLocator(HornetQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
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
   }

   public void testSSLWithIncorrectKeyStorePassword() throws Exception
   {
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, TransportConstants.DEFAULT_KEYSTORE_PATH);
      tc.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "invalid password");

      ServerLocator locator = addServerLocator(HornetQClient.createServerLocatorWithoutHA(tc));
      try
      {
         ClientSessionFactory sf = createSessionFactory(locator);
         addSessionFactory(sf);
         Assert.fail();
      }
      catch(NotConnectedException se)
      {
         //ok
      }
      catch (HornetQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
   }

   // see https://jira.jboss.org/jira/browse/HORNETQ-234
   public void testPlainConnectionToSSLEndpoint() throws Exception
   {
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, false);

      ServerLocator locator = addServerLocator(HornetQClient.createServerLocatorWithoutHA(tc));
      locator.setCallTimeout(2000);
      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
      try
      {
         sf.createSession(false, true, true);
         Assert.fail();
      }
      catch(NotConnectedException se)
      {
         //ok
      }
      catch (HornetQException e)
      {
         fail("Invalid Exception type:" + e.getType());
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
      config.getAcceptorConfigurations().add(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params));
      server = createServer(false, config);
      server.start();
      waitForServer(server);
      tc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
   }
}
