/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.integration.twitter;

import java.util.HashMap;

import junit.framework.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.config.TwitterConnectorConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.twitter.TwitterConnectorService;
import org.hornetq.core.twitter.TwitterConstants;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;
import twitter4j.*;

/**
 * A TwitterTest
 *
 * @author tm.igarashi@gmail.com
 *
 *
 */
public class TwitterTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(TwitterTest.class);
   private static final String KEY_CONNECTOR_NAME = "connector.name";
   private static final String KEY_USERNAME = "username";
   private static final String KEY_PASSWORD = "password";
   private static final String KEY_QUEUE_NAME = "queue.name";
   
   private static final String TWITTER_USERNAME = System.getProperty("twitter.username");
   private static final String TWITTER_PASSWORD = System.getProperty("twitter.password");
   
   @Override
   protected void setUp() throws Exception
   {
      if(TWITTER_USERNAME == null || TWITTER_PASSWORD == null)
      {
         throw new Exception("* * *  Please set twitter.username and twitter.password in system property  * * *");
      }
      super.setUp();
   }

   // incoming
   
   public void testSimpleIncoming() throws Exception
   {
      internalTestIncoming(true,false);
   }

   public void testIncomingNoQueue() throws Exception
   {
      internalTestIncoming(false,false);
   }

   public void testIncomingWithRestart() throws Exception
   {
      internalTestIncoming(true,true);
   }
   
   public void testIncomingWithEmptyConnectorName() throws Exception
   {
      HashMap<String,String> params = new HashMap<String,String>();
      params.put(KEY_CONNECTOR_NAME, "");
      internalTestIncomingFailedToInitialize(params);
   }

   public void testIncomingWithEmptyQueueName() throws Exception
   {
      HashMap<String,String> params = new HashMap<String,String>();
      params.put(KEY_QUEUE_NAME, "");
      internalTestIncomingFailedToInitialize(params);
   }

   public void testIncomingWithInvalidCredentials() throws Exception
   {
      HashMap<String,String> params = new HashMap<String,String>();
      params.put(KEY_USERNAME, "invalidUsername");
      params.put(KEY_PASSWORD, "invalidPassword");
      internalTestIncomingFailedToInitialize(params);
   }

   //outgoing
   
   public void testSimpleOutgoing() throws Exception
   {
      internalTestOutgoing(true,false);
   }

   public void testOutgoingNoQueue() throws Exception
   {
      internalTestOutgoing(false,false);
   }
   public void testOutgoingWithRestart() throws Exception
   {
      internalTestOutgoing(true,true);
   }
   
   public void testOutgoingWithEmptyConnectorName() throws Exception
   {
      HashMap<String,String> params = new HashMap<String,String>();
      params.put(KEY_CONNECTOR_NAME, "");
      internalTestOutgoingFailedToInitialize(params);
   }

   public void testOutgoingWithEmptyQueueName() throws Exception
   {
      HashMap<String,String> params = new HashMap<String,String>();
      params.put(KEY_QUEUE_NAME, "");
      internalTestOutgoingFailedToInitialize(params);
   }

   public void testOutgoingWithInvalidCredentials() throws Exception
   {
      HashMap<String,String> params = new HashMap<String,String>();
      params.put(KEY_USERNAME, "invalidUsername");
      params.put(KEY_PASSWORD, "invalidPassword");
      internalTestOutgoingFailedToInitialize(params);
   }
   
   /**
    *  This will fail until TFJ-347 is fixed.
    * http://twitter4j.org/jira/browse/TFJ-347
    * 
    * @throws Exception
    */
   public void _testOutgoingWithInReplyTo() throws Exception
   {
      internalTestOutgoingWithInReplyTo();
   }
   
   protected void internalTestIncoming(boolean createQueue, boolean restart) throws Exception
   {
      HornetQServer server0 = null;
      ClientSession session = null;
      String queue = "TwitterTestQueue";
      int interval = 5;
      Twitter twitter = new TwitterFactory().getInstance(TWITTER_USERNAME,TWITTER_PASSWORD);
      String testMessage = "TwitterTest/incoming: " + System.currentTimeMillis();
      log.debug("test incoming: " + testMessage);
      
      try
      {
         Configuration configuration = createDefaultConfig(false);
         TwitterConnectorConfiguration inconf = new TwitterConnectorConfiguration();
         inconf.setConnectorName("test-incoming-connector");
         inconf.setIncoming(true);
         inconf.setIntervalSeconds(interval);
         inconf.setQueueName(queue);
         inconf.setUserName(TWITTER_USERNAME);
         inconf.setPassword(TWITTER_PASSWORD);
         configuration.getTwitterConnectorConfigurations().add(inconf);
         if(createQueue)
         {
            CoreQueueConfiguration qc = new CoreQueueConfiguration(queue, queue, null, true);
            configuration.getQueueConfigurations().add(qc);
         }

         server0 = createServer(false,configuration);
         server0.start();
         
         TwitterConnectorService service = server0.getTwitterConnectorService();
         if(restart)
         {
            service.stop();
            service.start();
         }

         Assert.assertEquals(0, service.getOutgoingConnectorCount());
         if(createQueue)
         {
            Assert.assertEquals(1, service.getIncomingConnectorCount());
         }
         else
         {
            Assert.assertEquals(0, service.getIncomingConnectorCount());
            return;
         }

         twitter.updateStatus(testMessage);

         TransportConfiguration tpconf = new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY);
         ClientSessionFactory sf = HornetQClient.createClientSessionFactory(tpconf);
         session = sf.createSession(false, true, true);
         ClientConsumer consumer = session.createConsumer(queue);
         session.start();
         ClientMessage msg = consumer.receive(60*1000);
         
         Assert.assertNotNull(msg);
         Assert.assertEquals(testMessage, msg.getBodyBuffer().readString());
         
         msg.acknowledge();
      }
      finally
      {
         try
         {
            session.close();
         }
         catch(Throwable t)
         {
         }
         try
         {
            server0.stop();
         }
         catch(Throwable ignored)
         {
         }
      }
   }

   protected void internalTestIncomingFailedToInitialize(HashMap<String,String> params) throws Exception
   {
      HornetQServer server0 = null;
      String connectorName = "test-incoming-connector"; 
      String queue = "TwitterTestQueue";
      String userName = "invalidUsername";
      String password = "invalidPassword";
      int interval = 5;
      
      if(params.containsKey(KEY_CONNECTOR_NAME))
      {
         connectorName = params.get(KEY_CONNECTOR_NAME);
      }
      if(params.containsKey(KEY_USERNAME))
      {
         userName = params.get(KEY_USERNAME);
      }
      if(params.containsKey(KEY_PASSWORD))
      {
         password = params.get(KEY_PASSWORD);
      }
      if(params.containsKey(KEY_QUEUE_NAME))
      {
         queue = params.get(KEY_QUEUE_NAME);
      }
      
      try
      {
         Configuration configuration = createDefaultConfig(false);
         TwitterConnectorConfiguration inconf = new TwitterConnectorConfiguration();
         inconf.setConnectorName(connectorName);
         inconf.setIncoming(true);
         inconf.setIntervalSeconds(interval);
         inconf.setQueueName(queue);
         inconf.setUserName(userName);
         inconf.setPassword(password);
         configuration.getTwitterConnectorConfigurations().add(inconf);
         CoreQueueConfiguration qc = new CoreQueueConfiguration(queue, queue, null, true);
         configuration.getQueueConfigurations().add(qc);

         server0 = createServer(false,configuration);
         server0.start();
         
         TwitterConnectorService twitterService = server0.getTwitterConnectorService();
         Assert.assertEquals(0, twitterService.getIncomingConnectorCount());
         Assert.assertEquals(0, twitterService.getOutgoingConnectorCount());
      }
      finally
      {
         try
         {
            server0.stop();
         }
         catch(Throwable ignored)
         {
         }
      }
   }

   protected void internalTestOutgoing(boolean createQueue, boolean restart) throws Exception
   {
      HornetQServer server0 = null;
      ClientSession session = null;
      String queue = "TwitterTestQueue";
      Twitter twitter = new TwitterFactory().getInstance(TWITTER_USERNAME,TWITTER_PASSWORD);
      String testMessage = "TwitterTest/outgoing: " + System.currentTimeMillis();
      log.debug("test outgoing: " + testMessage);
      
      try
      {
         Configuration configuration = createDefaultConfig(false);
         TwitterConnectorConfiguration outconf = new TwitterConnectorConfiguration();
         outconf.setConnectorName("test-outgoing-connector");
         outconf.setIncoming(false);
         outconf.setQueueName(queue);
         outconf.setUserName(TWITTER_USERNAME);
         outconf.setPassword(TWITTER_PASSWORD);
         configuration.getTwitterConnectorConfigurations().add(outconf);
         if(createQueue)
         {
            CoreQueueConfiguration qc = new CoreQueueConfiguration(queue, queue, null, false);
            configuration.getQueueConfigurations().add(qc);
         }
         
         server0 = createServer(false,configuration);
         server0.start();
         
         TwitterConnectorService service = server0.getTwitterConnectorService();
         if(restart)
         {
            service.stop();
            service.start();
         }

         Assert.assertEquals(0, service.getIncomingConnectorCount());
         if(createQueue)
         {
            Assert.assertEquals(1, service.getOutgoingConnectorCount());
         }
         else
         {
            Assert.assertEquals(0, service.getOutgoingConnectorCount());
            return;
         }

         TransportConfiguration tpconf = new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY);
         ClientSessionFactory sf = HornetQClient.createClientSessionFactory(tpconf);
         session = sf.createSession(false, true, true);
         ClientProducer producer = session.createProducer(queue);
         ClientMessage msg = session.createMessage(false);
         msg.getBodyBuffer().writeString(testMessage);
         session.start();
         producer.send(msg);

         Thread.sleep(3000);
         
         Paging page = new Paging();
         page.setCount(1);
         ResponseList<Status> res = twitter.getHomeTimeline(page);
         
         Assert.assertEquals(testMessage, res.get(0).getText());
      }
      finally
      {
         try
         {
            session.close();
         }
         catch(Throwable t)
         {
         }
         try
         {
            server0.stop();
         }
         catch(Throwable ignored)
         {
         }
      }
   }

   protected void internalTestOutgoingFailedToInitialize(HashMap<String,String> params) throws Exception
   {
      HornetQServer server0 = null;
      String connectorName = "test-outgoing-connector"; 
      String queue = "TwitterTestQueue";
      String userName = TWITTER_USERNAME;
      String password = TWITTER_PASSWORD;
      
      if(params.containsKey(KEY_CONNECTOR_NAME))
      {
         connectorName = params.get(KEY_CONNECTOR_NAME);
      }
      if(params.containsKey(KEY_USERNAME))
      {
         userName = params.get(KEY_USERNAME);
      }
      if(params.containsKey(KEY_PASSWORD))
      {
         password = params.get(KEY_PASSWORD);
      }
      if(params.containsKey(KEY_QUEUE_NAME))
      {
         queue = params.get(KEY_QUEUE_NAME);
      }
      
      try
      {
         Configuration configuration = createDefaultConfig(false);
         TwitterConnectorConfiguration outconf = new TwitterConnectorConfiguration();
         outconf.setConnectorName(connectorName);
         outconf.setIncoming(false);
         outconf.setQueueName(queue);
         outconf.setUserName(userName);
         outconf.setPassword(password);
         configuration.getTwitterConnectorConfigurations().add(outconf);
         CoreQueueConfiguration qc = new CoreQueueConfiguration(queue, queue, null, false);
         configuration.getQueueConfigurations().add(qc);
         
         server0 = createServer(false,configuration);
         server0.start();
         
         TwitterConnectorService service = server0.getTwitterConnectorService();
         Assert.assertEquals(0, service.getIncomingConnectorCount());
         Assert.assertEquals(0, service.getOutgoingConnectorCount());
      }
      finally
      {
         try
         {
            server0.stop();
         }
         catch(Throwable ignored)
         {
         }
      }
   }

   protected void internalTestOutgoingWithInReplyTo() throws Exception
   {
      HornetQServer server0 = null;
      ClientSession session = null;
      String queue = "TwitterTestQueue";
      Twitter twitter = new TwitterFactory().getInstance(TWITTER_USERNAME,TWITTER_PASSWORD);
      String testMessage = "TwitterTest/outgoing with in_reply_to: " + System.currentTimeMillis();
      String replyMessage = "@" + TWITTER_USERNAME + " TwitterTest/outgoing reply: " + System.currentTimeMillis();
      try
      {
         Configuration configuration = createDefaultConfig(false);
         TwitterConnectorConfiguration outconf = new TwitterConnectorConfiguration();
         outconf.setConnectorName("test-outgoing-with-in-reply-to");
         outconf.setIncoming(false);
         outconf.setQueueName(queue);
         outconf.setUserName(TWITTER_USERNAME);
         outconf.setPassword(TWITTER_PASSWORD);
         configuration.getTwitterConnectorConfigurations().add(outconf);
         CoreQueueConfiguration qc = new CoreQueueConfiguration(queue, queue, null, false);
         configuration.getQueueConfigurations().add(qc);

         Status s = twitter.updateStatus(testMessage);

         server0 = createServer(false,configuration);
         server0.start();
         
         TransportConfiguration tpconf = new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY);
         ClientSessionFactory sf = HornetQClient.createClientSessionFactory(tpconf);
         session = sf.createSession(false, true, true);
         ClientProducer producer = session.createProducer(queue);
         ClientMessage msg = session.createMessage(false);
         msg.getBodyBuffer().writeString(replyMessage);
         msg.putLongProperty(TwitterConstants.KEY_IN_REPLY_TO_STATUS_ID, s.getId());
         session.start();
         producer.send(msg);

         Thread.sleep(3000);
         
         Paging page = new Paging();
         page.setCount(2);
         ResponseList<Status> res = twitter.getHomeTimeline(page);
         
         Assert.assertEquals(testMessage, res.get(1).getText());
         Assert.assertEquals(-1, res.get(1).getInReplyToStatusId());
         Assert.assertEquals(replyMessage, res.get(0).getText());
         Assert.assertEquals(s.getId(), res.get(0).getInReplyToStatusId());
      }
      finally
      {
         try
         {
            session.close();
         }
         catch(Throwable t)
         {
         }
         try
         {
            server0.stop();
         }
         catch(Throwable ignored)
         {
         }
      }
   }
}
