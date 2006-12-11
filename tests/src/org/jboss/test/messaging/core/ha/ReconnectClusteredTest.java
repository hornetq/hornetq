package org.jboss.test.messaging.core.ha;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossMessageConsumer;
import org.jboss.jms.client.JBossSession;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.ConsumerState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.message.MessageProxy;

import javax.jms.*;

/** Start two JBoss instances (clustered) to run these tests.
 *  */
public class ReconnectClusteredTest extends HATestBase
{

//    public void testSimpleReconnect() throws Exception
//    {
//        JBossConnection  conn = (JBossConnection)this.factoryServer1.createConnection();
//        ClientConnectionDelegate delegate = (ClientConnectionDelegate)conn.getDelegate();
//        ConnectionState state = (ConnectionState)delegate.getState();
//
//        assertFalse(state.isStarted());
//        conn.start();
//        assertTrue(state.isStarted());
//
//        JBossConnection conn2 = (JBossConnection)this.factoryServer1.createConnection();
//        conn.getDelegate().failOver(conn2.getDelegate());
//
//        conn.stop();
//        assertFalse(state.isStarted());
//
//    }
//
//    public void testSimpleReconnectWithClientID() throws Exception
//    {
//        JBossConnection  conn = (JBossConnection)this.factoryServer1.createConnection();
//        conn.setClientID("someClient");
//        ClientConnectionDelegate delegate = (ClientConnectionDelegate)conn.getDelegate();
//        ConnectionState state = (ConnectionState)delegate.getState();
//
//        assertFalse(state.isStarted());
//        conn.start();
//        assertTrue(state.isStarted());
//
//        JBossConnection conn2 = (JBossConnection)this.factoryServer1.createConnection();
//        conn.getDelegate().failOver(conn2.getDelegate());
//
//        // force recovering the clientID from server
//        state.setClientID(null);
//        assertEquals ("someClient",conn.getClientID());
//
//        conn.stop();
//        assertFalse(state.isStarted());
//
//    }
//
//    public void testWithSession() throws Exception
//    {
//        JBossConnection  conn = (JBossConnection)this.factoryServer1.createConnection();
//        Session session = conn.createSession(true,Session.AUTO_ACKNOWLEDGE);
//
//        ClientConnectionDelegate delegate = (ClientConnectionDelegate)conn.getDelegate();
//        ConnectionState state = (ConnectionState)delegate.getState();
//
//        JBossConnection conn2 = (JBossConnection)this.factoryServer1.createConnection();
//        conn.getDelegate().failOver(conn2.getDelegate());
//    }
//
//    public void testSimpleWithOneProducerOnTopic() throws Exception
//    {
//        JBossConnection  conn = (JBossConnection)this.factoryServer1.createConnection();
//        Session session = conn.createSession(false,Session.AUTO_ACKNOWLEDGE);
//        Destination destination = (Destination)getCtx1().lookup("topic/testTopic");
//        MessageProducer producer = session.createProducer(destination);
//
//        Message message = session.createTextMessage("Hello Before");
//        producer.send(message);
//
//        ClientConnectionDelegate delegate = (ClientConnectionDelegate)conn.getDelegate();
//        ConnectionState state = (ConnectionState)delegate.getState();
//
//        JBossConnection conn2 = (JBossConnection)this.factoryServer2.createConnection();
//        conn.getDelegate().failOver(conn2.getDelegate());
//
//        message = session.createTextMessage("Hello After");
//        producer.send(message);
//    }
//
//    public void testSimpleWithOneProducerOnQueue() throws Exception
//    {
//        JBossConnection  conn = (JBossConnection)this.factoryServer1.createConnection();
//        Session session = conn.createSession(false,Session.AUTO_ACKNOWLEDGE);
//        Destination destination = (Destination)getCtx1().lookup("queue/testQueue");
//        MessageProducer producer = session.createProducer(destination);
//
//        Message message = session.createTextMessage("Hello Before");
//        producer.send(message);
//
//        ClientConnectionDelegate delegate = (ClientConnectionDelegate)conn.getDelegate();
//        ConnectionState state = (ConnectionState)delegate.getState();
//
//        JBossConnection conn2 = (JBossConnection)this.factoryServer2.createConnection();
//        conn.getDelegate().failOver(conn2.getDelegate());
//
//        message = session.createTextMessage("Hello After");
//        producer.send(message);
//    }
//
//    public void testTopicCluster() throws Exception
//    {
//        log.info("++testTopicCluster");
//
//        log.info(">>Lookup Queue");
//        Destination destination = (Destination)getCtx1().lookup("topic/testDistributedTopic");
//
//        JBossConnection connFirstServer = (JBossConnection)this.factoryServer1.createConnection();
//        connFirstServer.start();
//        JBossSession sessionFirstServer = (JBossSession)connFirstServer.createSession(false,Session.AUTO_ACKNOWLEDGE);
//
//
//        JBossConnection connSecondServer = (JBossConnection)this.factoryServer2.createConnection();
//        connSecondServer.start();
//        JBossSession sessionSecondServer = (JBossSession)connSecondServer.createSession(false,Session.AUTO_ACKNOWLEDGE);
//
//        MessageProducer producer = sessionFirstServer.createProducer(destination);
//
//        MessageConsumer consumer = sessionSecondServer.createConsumer(destination);
//
//        producer.send(sessionFirstServer.createTextMessage("Hello"));
//
//        assertNotNull(consumer.receive(2000));
//    }
//
//    public void testTopicSubscriber() throws Exception
//    {
//        log.info("++testTopicSubscriber");
//
//        log.info(">>Lookup Queue");
//        Destination destination = (Destination)getCtx1().lookup("topic/testDistributedTopic");
//
//        log.info("Creating connection server1");
//        JBossConnection  conn = (JBossConnection)this.factoryServer1.createConnection();
//        conn.setClientID("testClient");
//        conn.start();
//
//        JBossSession session = (JBossSession)conn.createSession(true,Session.AUTO_ACKNOWLEDGE);
//        ClientSessionDelegate clientSessionDelegate = (ClientSessionDelegate)session.getDelegate();
//        SessionState sessionState = (SessionState)clientSessionDelegate.getState();
//
//        MessageConsumer consumerHA = session.createDurableSubscriber((Topic)destination,"T1");
//        JBossMessageConsumer jbossConsumerHA =(JBossMessageConsumer)consumerHA;
//
//        org.jboss.jms.client.delegate.ClientConsumerDelegate clientDelegate = (org.jboss.jms.client.delegate.ClientConsumerDelegate)jbossConsumerHA.getDelegate();
//        ConsumerState consumerState = (ConsumerState)clientDelegate.getState();
//
//        log.info("subscriptionName=" + consumerState.getSubscriptionName());
//
//
//        log.info(">>Creating Producer");
//        MessageProducer producer = session.createProducer(destination);
//        log.info(">>creating Message");
//        Message message = session.createTextMessage("Hello Before");
//        log.info(">>sending Message");
//        producer.send(message);
//        session.commit();
//
//        receiveMessage("consumerHA",consumerHA,true,false);
//
//        session.commit();
//        //if (true) return;
//
//        Object txID = sessionState.getCurrentTxId();
//
//        producer.send(session.createTextMessage("Hello again before failover"));
//
//        ClientConnectionDelegate delegate = (ClientConnectionDelegate)conn.getDelegate();
//
//        JMSRemotingConnection originalRemoting = delegate.getRemotingConnection();
//
//        log.info(">>Creating alternate connection");
//        JBossConnection conn2 = (JBossConnection)this.factoryServer2.createConnection();
//        log.info("NewConnectionCreated=" + conn2);
//
//        log.info(">>Failling over");
//        assertSame(originalRemoting,delegate.getRemotingConnection());
//        conn.getDelegate().failOver(conn2.getDelegate());
//
//        try {
//            originalRemoting.stop();
//        } catch (Throwable throwable) {
//            throwable.printStackTrace();
//        }
//
//
//        assertNotSame(originalRemoting,delegate.getRemotingConnection());
//
//        //System.out.println("Kill server1"); Thread.sleep(10000);
//
//        message = session.createTextMessage("Hello After");
//        log.info(">>Sending new message");
//        producer.send(message);
//
//        assertEquals(txID,sessionState.getCurrentTxId());
//        System.out.println("TransactionID on client = " + txID);
//        log.info(">>Final commit");
//
//       /* JBossConnection connSecondServer = (JBossConnection)this.factoryServer2.createConnection();
//        connSecondServer.start();
//        JBossSession sessionSecondServer = (JBossSession)connSecondServer.createSession(false,Session.AUTO_ACKNOWLEDGE);
//        MessageConsumer consumerSecondServer = sessionSecondServer.createConsumer(destination); */
//
//        session.commit();
//
//        /* receiveMessage("consumerSecondServer",consumerSecondServer,true,false);
//        receiveMessage("consumerSecondServer",consumerSecondServer,true,false);
//        receiveMessage("consumerSecondServer",consumerSecondServer,true,true); */
//
//        log.info("Calling alternate receiver");
//        receiveMessage("consumerHA",consumerHA,true,false);
//        receiveMessage("consumerHA",consumerHA,true,false);
//        receiveMessage("consumerHA",consumerHA,true,true);
//
//
//        session.commit();
//
//    }
//
//   public void testQueueHA() throws Exception
//   {
//       log.info("++testTopicSubscriber");
//
//       log.info(">>Lookup Queue");
//       Destination destination = (Destination)getCtx1().lookup("queue/testDistributedQueue");
//
//       log.info("Creating connection server1");
//       JBossConnection  conn = (JBossConnection)this.factoryServer1.createConnection();
//       conn.setClientID("testClient");
//       conn.start();
//
//       JBossSession session = (JBossSession)conn.createSession(true,Session.AUTO_ACKNOWLEDGE);
//       ClientSessionDelegate clientSessionDelegate = (ClientSessionDelegate)session.getDelegate();
//       SessionState sessionState = (SessionState)clientSessionDelegate.getState();
//
//       MessageConsumer consumerHA = session.createConsumer(destination);
//       JBossMessageConsumer jbossConsumerHA =(JBossMessageConsumer)consumerHA;
//
//       org.jboss.jms.client.delegate.ClientConsumerDelegate clientDelegate = (org.jboss.jms.client.delegate.ClientConsumerDelegate)jbossConsumerHA.getDelegate();
//       ConsumerState consumerState = (ConsumerState)clientDelegate.getState();
//
//       log.info("subscriptionName=" + consumerState.getSubscriptionName());
//
//
//       log.info(">>Creating Producer");
//       MessageProducer producer = session.createProducer(destination);
//       log.info(">>creating Message");
//       Message message = session.createTextMessage("Hello Before");
//       log.info(">>sending Message");
//       producer.send(message);
//       session.commit();
//
//       session.commit();
//       //if (true) return;
//
//       Object txID = sessionState.getCurrentTxId();
//
//       ClientConnectionDelegate delegate = (ClientConnectionDelegate)conn.getDelegate();
//
//       JMSRemotingConnection originalRemoting = delegate.getRemotingConnection();
//
//       log.info(">>Creating alternate connection");
//       JBossConnection conn2 = (JBossConnection)this.factoryServer2.createConnection();
//       log.info("NewConnectionCreated=" + conn2);
//
//       log.info(">>Failling over");
//       assertSame(originalRemoting,delegate.getRemotingConnection());
//       conn.getDelegate().failOver(conn2.getDelegate());
//
//       try {
//           originalRemoting.stop();
//       } catch (Throwable throwable) {
//           throwable.printStackTrace();
//       }
//
//
//       assertNotSame(originalRemoting,delegate.getRemotingConnection());
//
//       //System.out.println("Kill server1"); Thread.sleep(10000);
//       assertEquals(txID,sessionState.getCurrentTxId());
//       System.out.println("TransactionID on client = " + txID);
//       log.info(">>Final commit");
//
//       session.commit();
//
//       log.info("Calling alternate receiver");
//       receiveMessage("consumerHA",consumerHA,true,false);
//       receiveMessage("consumerHA",consumerHA,true,true);
//
//       session.commit();
//
//       for (int i=0;i<30;i++)
//       {
//          log.info("Message Sent " + i);
//          producer.send(session.createTextMessage("Message " + i));
//       }
//      session.commit();
//
//      Thread.sleep(5000);
//
//       TextMessage messageLoop = null;
//       while (!((messageLoop = (TextMessage) consumerHA.receive(5000)) == null))
//       {
//          log.info("Message received = " + messageLoop.getText());
//       }
//
//   }
//
//
//    private void receiveMessage(String text, MessageConsumer consumer, boolean shouldAssert, boolean shouldBeNull) throws Exception
//    {
//        MessageProxy message = (MessageProxy)consumer.receive(3000);
//        TextMessage txtMessage = (TextMessage)message;
//        if (message!=null)
//        {
//            log.info(text + ": messageID from messageReceived=" + message.getMessage().getMessageID() + " message = " + message + " content=" + txtMessage.getText());
//        }
//        else
//        {
//            log.info(text + ": Message received was null");
//        }
//        if (shouldAssert)
//        {
//            if (shouldBeNull)
//            {
//                assertNull(message);
//            }
//            else
//            {
//                assertNotNull(message);
//            }
//        }
//    }
//
//   public void testSimplestDurableSubscription() throws Exception
//   {
//      ConnectionFactory cf = factoryServer1;
//      Topic topic = (Topic)getCtx1().lookup("topic/testDistributedTopic");
//
//      Connection conn = cf.createConnection();
//
//      conn.setClientID("brookeburke");
//
//      conn.start();
//
//      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//      MessageProducer prod = s.createProducer(topic);
//      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
//
//      log.info("      ******************                       s.createDurableSubscriber(topic, \"monicabelucci\")                 ****************************");
//      MessageConsumer firstconsumer = (MessageConsumer)s.createDurableSubscriber(topic, "monicabelucci");
//
//
//      System.out.println("\n\n****************************************   consumer=" + firstconsumer.toString() + "\n\n");
//      Object message=firstconsumer.receive(2000);
//
//      log.info(" \n\n*******************************                          message Received=" + message + "\n\n");
//
//      prod.send(s.createTextMessage("k"));
//
//      conn.close();
//
//      conn = cf.createConnection();
//      conn.setClientID("brookeburke");
//
//      log.info("      ******************                       conn.createSession(...);                                                       ****************************");
//      s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//
//      log.info("      ******************                       s.createDurableSubscriber(topic, \"monicabelucci\")                 ****************************");
//      MessageConsumer durable = s.createDurableSubscriber(topic, "monicabelucci");
//
//       log.info("      ******************                       conn.start();                                                       ****************************");
//       conn.start();
//
//      log.info("      ******************                       TextMessage tm = (TextMessage)durable.receive();                    ****************************");
//      TextMessage tm = (TextMessage)durable.receive();
//      assertEquals("k", tm.getText());
//
//      log.info("      ******************                       Message m = durable.receive(1000); (expected to be null)            ****************************");
//      Message m = durable.receive(1000);
//
//       prod = s.createProducer(topic);
//       prod.setDeliveryMode(DeliveryMode.PERSISTENT);
//       prod.send(s.createTextMessage("Hello"));
//
//       Connection conn2 = factoryServer2.createConnection();
//       conn2.setClientID("clientTest");
//
//       Session session = conn2.createSession(false,Session.AUTO_ACKNOWLEDGE);
//       conn2.start();
//
//       MessageConsumer consumer2 = session.createDurableSubscriber(topic,"spongebog");
//       System.out.println("Consumer2=" + consumer2);
//
//
//   }


}
