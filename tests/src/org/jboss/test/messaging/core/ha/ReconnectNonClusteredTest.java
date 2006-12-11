package org.jboss.test.messaging.core.ha;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossSession;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.TextMessageProxy;

import javax.jms.*;


/** Start two JBoss instances (non clustered) to run these tests.
 *  */
public class ReconnectNonClusteredTest extends HATestBase
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
//        System.out.println("Kill server1");
//        Thread.sleep(10000);
//
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
//        System.out.println("Kill server1");
//        Thread.sleep(10000);
//
//
//        message = session.createTextMessage("Hello After");
//        producer.send(message);
//    }
//
//    public void testSimpleWithOneProducerTransacted() throws Exception
//    {
//        log.info("++testSimpleWithOneProducerTransacted");
//
//        log.info(">>Lookup Queue");
//        Destination destination = (Destination)getCtx1().lookup("topic/testTopic");
//
//        log.info("Creating connections used for assertion (not failed over)");
//        JBossConnection connSecondServer = (JBossConnection)this.factoryServer2.createConnection();
//        connSecondServer.start();
//        JBossSession sessionSecondServer = (JBossSession)connSecondServer.createSession(false,Session.AUTO_ACKNOWLEDGE);
//        MessageConsumer consumerSecondServer = sessionSecondServer.createConsumer(destination);
//
//        JBossConnection connFirstServer = (JBossConnection)this.factoryServer1.createConnection();
//        connFirstServer.start();
//        JBossSession sessionFirstServer = (JBossSession)connFirstServer.createSession(false,Session.AUTO_ACKNOWLEDGE);
//        MessageConsumer consumerFirstServer = sessionFirstServer.createConsumer(destination);
//
//
//        log.info("Creating connection server1");
//        JBossConnection  conn = (JBossConnection)this.factoryServer1.createConnection();
//
//        log.info("ConnectionCreated=" + conn);
//        log.info(">>Creating Sessions");
//
//        JBossSession session = (JBossSession)conn.createSession(true,Session.AUTO_ACKNOWLEDGE);
//        ClientSessionDelegate clientSessionDelegate = (ClientSessionDelegate)session.getDelegate();
//        SessionState sessionState = (SessionState)clientSessionDelegate.getState();
//        log.info(">>Creating Producer");
//        MessageProducer producer = session.createProducer(destination);
//        log.info(">>Creating Producer - ");
//        log.info(">>creating Message");
//        Message message = session.createTextMessage("Hello Before");
//        log.info(">>sending Message");
//        producer.send(message);
//
//        assertNull(consumerFirstServer.receive(1000));
//        assertNull(consumerSecondServer.receive(1000));
//
//        log.info("sending first commit");
//        session.commit();
//        Object txID = sessionState.getCurrentTxId();
//
//        assertNotNull(consumerFirstServer.receive(2000));
//        assertNull(consumerFirstServer.receive(1000));
//        assertNull(consumerSecondServer.receive(1000));
//
//        producer.send(session.createTextMessage("Hello again before failover"));
//        assertNull(consumerFirstServer.receive(1000));
//        assertNull(consumerSecondServer.receive(1000));
//
//        ClientConnectionDelegate delegate = (ClientConnectionDelegate)conn.getDelegate();
//
//        JMSRemotingConnection originalRemoting = delegate.getRemotingConnection();
//        ConnectionState state = (ConnectionState)delegate.getState();
//
//        log.info(">>Creating alternate connection");
//        JBossConnection conn2 = (JBossConnection)this.factoryServer2.createConnection();
//        log.info("NewConnectionCreated=" + conn2);
//
//        log.info(">>Failling over");
//        assertSame(originalRemoting,delegate.getRemotingConnection());
//        conn.getDelegate().failOver(conn2.getDelegate());
//        /*try {
//            originalRemoting.stop();
//        } catch (Throwable throwable) {
//            throwable.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        } */
//
//        assertNotSame(originalRemoting,delegate.getRemotingConnection());
//
//        //System.out.println("Kill server1"); Thread.sleep(10000);
//
//        message = session.createTextMessage("Hello After");
//        log.info(">>Sending new message");
//        producer.send(message);
//
//        assertNull(consumerFirstServer.receive(1000));
//        assertNull(consumerSecondServer.receive(1000));
//
//        assertEquals(txID,sessionState.getCurrentTxId());
//        System.out.println("TransactionID on client = " + txID);
//        log.info(">>Final commit");
//        session.commit();
//
//        log.info("Checking receive on second server");
//        assertNotNull(consumerSecondServer.receive(1000));
//        assertNotNull(consumerSecondServer.receive(1000));
//        log.info("Checking receive on first server");
//        assertNull(consumerFirstServer.receive(1000));
//        assertNull(consumerSecondServer.receive(1000));
//
//    }
//
//    public void testSimpleWithOneProducerTransactedWithoutHA() throws Exception
//    {
//        log.info("++testSimpleWithOneProducerTransacted");
//
//        log.info(">>Lookup Queue");
//        Destination destination = (Destination)getCtx1().lookup("topic/testTopic");
//
//        log.info("Creating connections used for assertion (not failed over)");
//        JBossConnection connSecondServer = (JBossConnection)this.factoryServer2.createConnection();
//        connSecondServer.start();
//        JBossSession sessionSecondServer = (JBossSession)connSecondServer.createSession(false,Session.AUTO_ACKNOWLEDGE);
//        MessageConsumer consumerSecondServer = sessionSecondServer.createConsumer(destination);
//
//        JBossConnection connFirstServer = (JBossConnection)this.factoryServer1.createConnection();
//        connFirstServer.start();
//        JBossSession sessionFirstServer = (JBossSession)connFirstServer.createSession(false,Session.AUTO_ACKNOWLEDGE);
//        MessageConsumer consumerFirstServer = sessionFirstServer.createConsumer(destination);
//
//
//        log.info("Creating connection server1");
//        JBossConnection  conn = (JBossConnection)this.factoryServer2.createConnection();
//
//        log.info("ConnectionCreated=" + conn);
//        log.info(">>Creating Sessions");
//
//        JBossSession session = (JBossSession)conn.createSession(true,Session.AUTO_ACKNOWLEDGE);
//        ClientSessionDelegate clientSessionDelegate = (ClientSessionDelegate)session.getDelegate();
//        SessionState sessionState = (SessionState)clientSessionDelegate.getState();
//        System.out.println("Size of callbackHandlers=" + sessionState.getCallbackHandlers().size());
//        Object txID = sessionState.getCurrentTxId();
//        log.info(">>Creating Producer");
//        MessageProducer producer = session.createProducer(destination);
//        log.info(">>Creating Producer - ");
//        log.info(">>creating Message");
//        Message message = session.createTextMessage("Hello Before");
//        log.info(">>sending Message");
//        producer.send(message);
//
//        assertNull(consumerFirstServer.receive(1000));
//        assertNull(consumerSecondServer.receive(1000));
//
//        log.info("sending first commit");
//        //session.commit();
//
//        assertNull(consumerFirstServer.receive(2000));
//        assertNull(consumerFirstServer.receive(1000));
//        assertNull(consumerSecondServer.receive(1000));
//
//        TextMessageProxy messagetxt = (TextMessageProxy)session.createTextMessage("Hello again before failover");
//        producer.send(messagetxt);
//        System.out.println("Id=" + messagetxt.getMessage().getConnectionID());
//        assertNull(consumerFirstServer.receive(1000));
//        assertNull(consumerSecondServer.receive(1000));
//
//        ClientConnectionDelegate delegate = (ClientConnectionDelegate)conn.getDelegate();
//
//        JMSRemotingConnection originalRemoting = delegate.getRemotingConnection();
//        ConnectionState state = (ConnectionState)delegate.getState();
//
//        log.info(">>Failling over");
//        //System.out.println("Kill server1"); Thread.sleep(10000);
//
//        message = session.createTextMessage("Hello After");
//        log.info(">>Sending new message");
//        producer.send(message);
//
//        assertNull(consumerFirstServer.receive(1000));
//        assertNull(consumerSecondServer.receive(1000));
//
//        assertEquals(txID,sessionState.getCurrentTxId());
//        System.out.println("TransactionID on client = " + txID);
//        log.info(">>Final commit");
//        session.commit();
//
//        log.info("Checking receive on second server");
//        assertNull(consumerFirstServer.receive(1000));
//        assertNotNull(consumerSecondServer.receive(3000));
//        assertNotNull(consumerSecondServer.receive(1000));
//        assertNotNull(consumerSecondServer.receive(1000));
//        log.info("Checking receive on first server");
//        assertNull(consumerSecondServer.receive(1000));
//
//    }
//
//    public void testTopicSubscriber() throws Exception
//    {
//        log.info("++testSimpleWithOneProducerTransacted");
//
//        log.info(">>Lookup Queue");
//        Destination destination = (Destination)getCtx1().lookup("topic/testTopic");
//
//        JBossConnection connFirstServer = (JBossConnection)this.factoryServer1.createConnection();
//        connFirstServer.start();
//        JBossSession sessionFirstServer = (JBossSession)connFirstServer.createSession(false,Session.AUTO_ACKNOWLEDGE);
//
//
//        log.info("Creating connection server1");
//        JBossConnection  conn = (JBossConnection)this.factoryServer1.createConnection();
//        conn.start();
//
//        log.info("ConnectionCreated=" + conn);
//        log.info(">>Creating Sessions");
//
//        JBossSession session = (JBossSession)conn.createSession(true,Session.AUTO_ACKNOWLEDGE);
//        ClientSessionDelegate clientSessionDelegate = (ClientSessionDelegate)session.getDelegate();
//        SessionState sessionState = (SessionState)clientSessionDelegate.getState();
//        MessageConsumer consumerHA = session.createConsumer(destination);
//        log.info(">>Creating Producer");
//        MessageProducer producer = session.createProducer(destination);
//        log.info(">>creating Message");
//        Message message = session.createTextMessage("Hello Before");
//        log.info(">>sending Message");
//        producer.send(message);
//        session.commit();
//
//        assertNotNull(consumerHA.receive(3000));
//        session.commit();
//
//        Object txID = sessionState.getCurrentTxId();
//
//        producer.send(session.createTextMessage("Hello again before failover"));
//
//        ClientConnectionDelegate delegate = (ClientConnectionDelegate)conn.getDelegate();
//
//        JMSRemotingConnection originalRemoting = delegate.getRemotingConnection();
//
//        ConnectionState state = (ConnectionState)delegate.getState();
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
//        JBossConnection connSecondServer = (JBossConnection)this.factoryServer2.createConnection();
//        connSecondServer.start();
//        JBossSession sessionSecondServer = (JBossSession)connSecondServer.createSession(false,Session.AUTO_ACKNOWLEDGE);
//        MessageConsumer consumerSecondServer = sessionSecondServer.createConsumer(destination);
//
//        session.commit();
//
//        //assertNotNull(consumerSecondServer.receive(3000));
//        assertNotNull(consumerSecondServer.receive(3000));
//        assertNotNull(consumerSecondServer.receive(3000));
//        assertNull(consumerSecondServer.receive(3000));
//
//        log.info("Calling alternate receiver");
//        //assertNotNull(consumerHA.receive(1000));
//        assertNotNull(consumerHA.receive(1000));
//        assertNotNull(consumerHA.receive(1000));
//        assertNull(consumerHA.receive(1000));
//
//    }
}
