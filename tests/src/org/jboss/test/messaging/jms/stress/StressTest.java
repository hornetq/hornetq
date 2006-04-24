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
package org.jboss.test.messaging.jms.stress;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XASession;

/**
 * 
 * A StressTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * StressTest.java,v 1.1 2006/04/24 13:14:31 timfox Exp
 */
public class StressTest extends StressTestBase
{
   public StressTest(String name)
   {
      super(name);
   }
   
   public void setUp() throws Exception
   {
      super.setUp();      
   }
   
   public void tearDown() throws Exception
   {     
      super.tearDown();            
   }
   
   /*
    * Stress a queue with transational, non transactional and 2pc senders sending both persistent
    * and non persistent messages
    * Transactional senders go through a cycle of sending and rolling back
    * 
    */
   public void testQueueMultipleSenders() throws Exception
   {
      Connection conn1 = cf.createConnection();
      
      Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess2 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess3 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess4 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess5 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess6 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess7 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess8 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      Session sess9 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess10 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess11 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess12 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess13 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess14 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess15 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess16 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      
      XASession xaSess1 = ((XAConnection)conn1).createXASession();
      XASession xaSess2 = ((XAConnection)conn1).createXASession();
      XASession xaSess3 = ((XAConnection)conn1).createXASession();
      XASession xaSess4 = ((XAConnection)conn1).createXASession();
      XASession xaSess5 = ((XAConnection)conn1).createXASession();
      XASession xaSess6 = ((XAConnection)conn1).createXASession();
      XASession xaSess7 = ((XAConnection)conn1).createXASession();
      XASession xaSess8 = ((XAConnection)conn1).createXASession();
      
      Session sess17 = xaSess1.getSession();
      Session sess18 = xaSess2.getSession();
      Session sess19 = xaSess3.getSession();
      Session sess20 = xaSess4.getSession();
      Session sess21 = xaSess5.getSession();
      Session sess22 = xaSess6.getSession();
      Session sess23 = xaSess7.getSession();
      Session sess24 = xaSess8.getSession();
      
      MessageProducer prod1 = sess1.createProducer(queue1);
      prod1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod2 = sess2.createProducer(queue1);
      prod2.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod3 = sess3.createProducer(queue1);
      prod3.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod4 = sess4.createProducer(queue1);
      prod4.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod5 = sess5.createProducer(queue1);
      prod5.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod6 = sess6.createProducer(queue1);
      prod6.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod7 = sess7.createProducer(queue1);
      prod7.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod8 = sess8.createProducer(queue1);
      prod8.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod9 = sess9.createProducer(queue1);
      prod9.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod10 = sess10.createProducer(queue1);
      prod10.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod11 = sess11.createProducer(queue1);
      prod11.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod12 = sess12.createProducer(queue1);
      prod12.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod13 = sess13.createProducer(queue1);
      prod13.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod14 = sess14.createProducer(queue1);
      prod14.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod15 = sess15.createProducer(queue1);
      prod15.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod16 = sess16.createProducer(queue1);
      prod16.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod17 = sess17.createProducer(queue1);
      prod17.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod18 = sess18.createProducer(queue1);
      prod18.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod19 = sess19.createProducer(queue1);
      prod19.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod20 = sess20.createProducer(queue1);
      prod20.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod21 = sess21.createProducer(queue1);
      prod21.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod22 = sess22.createProducer(queue1);
      prod22.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod23 = sess23.createProducer(queue1);
      prod23.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod24 = sess24.createProducer(queue1);
      prod24.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
            
      Connection conn2 = cf.createConnection();
      conn2.start();
      Session sessReceive = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      
  
      Runner[] runners = new Runner[] {
      new Sender("prod1", sess1, prod1, NUM_NON_PERSISTENT_MESSAGES),
      new Sender("prod2", sess2, prod2, NUM_PERSISTENT_MESSAGES),
      new Sender("prod3", sess3, prod3, NUM_NON_PERSISTENT_MESSAGES),
      new Sender("prod4", sess4, prod4, NUM_PERSISTENT_MESSAGES),
      new Sender("prod5", sess5, prod5, NUM_NON_PERSISTENT_MESSAGES),
      new Sender("prod6", sess6, prod6, NUM_PERSISTENT_MESSAGES),
      new Sender("prod7", sess7, prod7, NUM_NON_PERSISTENT_MESSAGES),
      new Sender("prod8", sess8, prod8, NUM_PERSISTENT_MESSAGES),
      new TransactionalSender("prod9", sess9, prod9, NUM_NON_PERSISTENT_MESSAGES, 1, 1),
      new TransactionalSender("prod10", sess10, prod10, NUM_PERSISTENT_MESSAGES, 1, 1),
      new TransactionalSender("prod11", sess11, prod11, NUM_NON_PERSISTENT_MESSAGES, 10, 7),
      new TransactionalSender("prod12", sess12, prod12, NUM_PERSISTENT_MESSAGES, 10, 7),
      new TransactionalSender("prod13", sess13, prod13, NUM_NON_PERSISTENT_MESSAGES, 50, 21),
      new TransactionalSender("prod14", sess14, prod14, NUM_PERSISTENT_MESSAGES, 50, 21),
      new TransactionalSender("prod15", sess15, prod15, NUM_NON_PERSISTENT_MESSAGES, 100, 67),
      new TransactionalSender("prod16", sess16, prod16, NUM_PERSISTENT_MESSAGES, 100, 67),            
      new Transactional2PCSender("prod17", xaSess1, prod17, NUM_NON_PERSISTENT_MESSAGES, 1, 1),
      new Transactional2PCSender("prod18", xaSess2, prod18, NUM_PERSISTENT_MESSAGES, 1, 1),
      new Transactional2PCSender("prod19", xaSess3, prod19, NUM_NON_PERSISTENT_MESSAGES, 10, 7),
      new Transactional2PCSender("prod20", xaSess4, prod20, NUM_PERSISTENT_MESSAGES, 10, 7),
      new Transactional2PCSender("prod21", xaSess5, prod21, NUM_NON_PERSISTENT_MESSAGES, 50, 21),
      new Transactional2PCSender("prod22", xaSess6, prod22, NUM_PERSISTENT_MESSAGES, 50, 21),
      new Transactional2PCSender("prod23", xaSess7, prod23, NUM_NON_PERSISTENT_MESSAGES, 100, 67),
      new Transactional2PCSender("prod24", xaSess8, prod24, NUM_PERSISTENT_MESSAGES, 100, 67), 
      new Receiver(sessReceive, cons, 12 * NUM_NON_PERSISTENT_MESSAGES + 12 * NUM_PERSISTENT_MESSAGES, false) };
      
      runRunners(runners);
      
      conn1.close();
      
      conn2.close();      
   }
   
   /*
    * Stress a topic with with many non transactional, transactional and 2pc receivers.
    * Non transactional receivers use ack modes of auto, dups and client ack.
    * Client ack receivers go through a cycle of receving a batch, acking and recovering
    * Transactional receivers go through a cycle of receiving commiting and rolling back.
    * Half the consumers are durable and half non durable.
    * 
    */
   public void testTopicMultipleReceivers() throws Exception
   {
      Connection conn1 = cf.createConnection();
      
      Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess2 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer prod1 = sess1.createProducer(topic1);
      prod1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      MessageProducer prod2 = sess2.createProducer(topic1);
      prod2.setDeliveryMode(DeliveryMode.PERSISTENT);
                  
      Connection conn2 = cf.createConnection();
      conn2.setClientID("clientid1");
      conn2.start();
      
      //4 auto ack
      Session rsess1 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session rsess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session rsess3 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session rsess4 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      //4 dups
      Session rsess5 = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      Session rsess6 = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      Session rsess7 = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      Session rsess8 = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      //4 client
      Session rsess9 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      Session rsess10 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      Session rsess11 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      Session rsess12 = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      
      //4 transactional
      Session rsess13 = conn2.createSession(true, Session.SESSION_TRANSACTED);
      Session rsess14 = conn2.createSession(true, Session.SESSION_TRANSACTED);
      Session rsess15 = conn2.createSession(true, Session.SESSION_TRANSACTED);
      Session rsess16 = conn2.createSession(true, Session.SESSION_TRANSACTED);
      
      //4 2pc transactional
      XASession rxaSess1 = ((XAConnection)conn2).createXASession();
      XASession rxaSess2 = ((XAConnection)conn2).createXASession();
      XASession rxaSess3 = ((XAConnection)conn2).createXASession();
      XASession rxaSess4 = ((XAConnection)conn2).createXASession();
         
      Session rsess17 = rxaSess1.getSession();
      Session rsess18 = rxaSess2.getSession();
      Session rsess19 = rxaSess3.getSession();
      Session rsess20 = rxaSess4.getSession();

                      
      MessageConsumer cons1 = rsess1.createConsumer(topic1);
      MessageConsumer cons2 = rsess2.createDurableSubscriber(topic1, "sub1");
      MessageConsumer cons3 = rsess3.createConsumer(topic1);
      MessageConsumer cons4 = rsess4.createDurableSubscriber(topic1, "sub2");
      MessageConsumer cons5 = rsess5.createConsumer(topic1);
      MessageConsumer cons6 = rsess6.createDurableSubscriber(topic1, "sub3");
      MessageConsumer cons7 = rsess7.createConsumer(topic1);
      MessageConsumer cons8 = rsess8.createDurableSubscriber(topic1, "sub4");
      MessageConsumer cons9 = rsess9.createConsumer(topic1);
      MessageConsumer cons10 = rsess10.createDurableSubscriber(topic1, "sub5");
      MessageConsumer cons11 = rsess11.createConsumer(topic1);
      MessageConsumer cons12 = rsess12.createDurableSubscriber(topic1, "sub6");
      MessageConsumer cons13 = rsess13.createConsumer(topic1);
      MessageConsumer cons14 = rsess14.createDurableSubscriber(topic1, "sub7");
      MessageConsumer cons15 = rsess15.createConsumer(topic1);
      MessageConsumer cons16 = rsess16.createDurableSubscriber(topic1, "sub8");
      MessageConsumer cons17 = rsess17.createConsumer(topic1);
      MessageConsumer cons18 = rsess18.createDurableSubscriber(topic1, "sub9");
      MessageConsumer cons19 = rsess19.createConsumer(topic1);
      MessageConsumer cons20 = rsess20.createDurableSubscriber(topic1, "sub10");

      
      //To make sure paging occurs first send the messages, then receive
  
      Runner[] runners = new Runner[] {
            
      new Sender("prod1", sess1, prod1, NUM_NON_PERSISTENT_MESSAGES),
      new Sender("prod2", sess2, prod2, NUM_PERSISTENT_MESSAGES)
      };
      
      runRunners(runners);
      
      runners = new Runner[] {
      //4 auto ack
      new Receiver(rsess1, cons1, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, false),
      new Receiver(rsess2, cons2, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, true),
      new Receiver(rsess3, cons3, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, false),
      new Receiver(rsess4, cons4, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, true),
      
      //4 dups ok
      new Receiver(rsess5, cons5, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, false),
      new Receiver(rsess6, cons6, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, true),
      new Receiver(rsess7, cons7, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, false),
      new Receiver(rsess8, cons8, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, true),
      
      //4 client ack
      
      new RecoveringReceiver(rsess9, cons9, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, 1, 1, false),
      new RecoveringReceiver(rsess10, cons10, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, 10, 7, true),
      new RecoveringReceiver(rsess11, cons11, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, 50, 21, false),
      new RecoveringReceiver(rsess12, cons12, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, 100, 67, true),
       
      //4 transactional
      
      new TransactionalReceiver(rsess13, cons13, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, 1, 1, false),
      new TransactionalReceiver(rsess14, cons14, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, 10, 7, true),
      new TransactionalReceiver(rsess15, cons15, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, 50, 21, false),
      new TransactionalReceiver(rsess16, cons16, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, 100, 67, true),
      
      //4 2pc transactional
      new Transactional2PCReceiver(rxaSess1, cons17, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, 1, 1, false),
      new Transactional2PCReceiver(rxaSess2, cons18, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, 10, 7, true),
      new Transactional2PCReceiver(rxaSess3, cons19, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, 50, 21, false),
      new Transactional2PCReceiver(rxaSess4, cons20, NUM_NON_PERSISTENT_MESSAGES + NUM_PERSISTENT_MESSAGES, 100, 67, true),
       
      };
      
      runRunners(runners);
      
      conn1.close();
      
      conn2.close();      
   }    
   
   public void testConnectionConsumer() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, 100000),
                                        new Receiver(conn, sessReceive, 100000, queue1) };

      runRunners(runners);

      conn.close();      
   }

}
