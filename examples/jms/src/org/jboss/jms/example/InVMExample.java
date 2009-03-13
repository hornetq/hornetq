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
package org.jboss.jms.example;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class InVMExample
{
   public static void main(String[] args) throws Exception
   {
      MessagingService messagingService = null;
      ConfigurationImpl configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      messagingService = Messaging.newNullStorageMessagingService(configuration);
      //start the server
      messagingService.start();
      JMSServerManagerImpl serverManager = JMSServerManagerImpl.newJMSServerManagerImpl(messagingService.getServer());
      serverManager.start();
      //if you want to lookup the objects via jndi, use a proper context
      serverManager.setContext(new NullInitialContext());
      ConnectionFactory cf = new JBossConnectionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()));

      Connection conn = cf.createConnection();
      conn.setClientID("myid");
      //some queue stuff
      Queue q = new JBossQueue("queueA");
      serverManager.createQueue(q.getQueueName(), q.getQueueName());
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess.createProducer(q);
      prod.send(sess.createTextMessage("hello"));
      MessageConsumer cons = sess.createConsumer(q, null, false);
      conn.start();
      TextMessage message = (TextMessage) cons.receive(5000);
      System.out.println("message.getText() = " + message.getText());

      //some topic stuff
      Topic topic = new JBossTopic("topicA");
      serverManager.createTopic(topic.getTopicName(), topic.getTopicName());
      prod = sess.createProducer(topic);
      TopicSubscriber sub = sess.createDurableSubscriber(topic, "mySub", null, false);
      prod.send(sess.createTextMessage("hello again"));
      message = (TextMessage)sub.receive(5000);
      System.out.println("message.getText() = " + message.getText());

      conn.close();
      //stop the server
      messagingService.stop();
   }

   static class NullInitialContext extends InitialContext
   {

      public NullInitialContext() throws NamingException
      {
         super();
      }

      @Override
      public void rebind(String name, Object obj) throws NamingException
      {
      }
   }

}
