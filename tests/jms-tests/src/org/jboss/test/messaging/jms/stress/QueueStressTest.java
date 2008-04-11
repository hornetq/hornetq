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
 * A QueueStressTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2349 $</tt>
 *
 * $Id: StressTest.java 2349 2007-02-19 14:15:53Z timfox $
 */
public class QueueStressTest extends StressTestBase
{
   public QueueStressTest(String name)
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
      tweakXASession(xaSess1);
      XASession xaSess2 = ((XAConnection)conn1).createXASession();
      tweakXASession(xaSess2);
      XASession xaSess3 = ((XAConnection)conn1).createXASession();
      tweakXASession(xaSess3);
      XASession xaSess4 = ((XAConnection)conn1).createXASession();
      tweakXASession(xaSess4);
      XASession xaSess5 = ((XAConnection)conn1).createXASession();
      tweakXASession(xaSess5);
      XASession xaSess6 = ((XAConnection)conn1).createXASession();
      tweakXASession(xaSess6);
      XASession xaSess7 = ((XAConnection)conn1).createXASession();
      tweakXASession(xaSess7);
      XASession xaSess8 = ((XAConnection)conn1).createXASession();
      tweakXASession(xaSess8);
      
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
      prod2.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod3 = sess3.createProducer(queue1);
      prod3.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod4 = sess4.createProducer(queue1);
      prod4.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod5 = sess5.createProducer(queue1);
      prod5.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod6 = sess6.createProducer(queue1);
      prod6.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod7 = sess7.createProducer(queue1);
      prod7.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod8 = sess8.createProducer(queue1);
      prod8.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod9 = sess9.createProducer(queue1);
      prod9.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod10 = sess10.createProducer(queue1);
      prod10.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod11 = sess11.createProducer(queue1);
      prod11.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod12 = sess12.createProducer(queue1);
      prod12.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod13 = sess13.createProducer(queue1);
      prod13.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod14 = sess14.createProducer(queue1);
      prod14.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod15 = sess15.createProducer(queue1);
      prod15.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod16 = sess16.createProducer(queue1);
      prod16.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod17 = sess17.createProducer(queue1);
      prod17.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod18 = sess18.createProducer(queue1);
      prod18.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod19 = sess19.createProducer(queue1);
      prod19.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod20 = sess20.createProducer(queue1);
      prod20.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod21 = sess21.createProducer(queue1);
      prod21.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod22 = sess22.createProducer(queue1);
      prod22.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod23 = sess23.createProducer(queue1);
      prod23.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod24 = sess24.createProducer(queue1);
      prod24.setDeliveryMode(DeliveryMode.PERSISTENT);


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
      new Receiver(sessReceive, cons,
                   12 * NUM_NON_PERSISTENT_MESSAGES + 12 * NUM_PERSISTENT_MESSAGES, false) };
      
      runRunners(runners);
      
      conn1.close();
      
      conn2.close();      
   }

}

