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
 * Stress tests for JMS queues using different connections for each session
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * StressQueueNotSameConnection.java,v 1.1 2006/01/17 12:15:33 timfox Exp
 */
public class StressQueueNotSameConnection extends StressTestBase
{
   
   public StressQueueNotSameConnection(String name)
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
    * Permutations for sender
    * T/NT/X
    * P/NP
    * 
    * Permutations for receiver
    * L/NL
    * X/T/CA/A/D
    *
    * T = Transacted
    * X = 2PC transacted
    * NT = Non Transacted
    * P = Persistent
    * NP = Non-persistent
    * CA = Client Acl
    * A = Auto ack
    * D = Dups ok ack
    *
    * Permutations
    *
    * TP-LX
    * TP-LT
    * TP-LCA
    * TP-LA
    * TP-LD
    * 
    * TP-NLX
    * TP-NLT
    * TP-NLCA
    * TP-NLA
    * TP-NLD
    * 
    * TNP-LX
    * TNP-LT
    * TNP-LCA
    * TNP-LA
    * TNP-LD
    * 
    * TNP-NLX
    * TNP-NLT
    * TNP-NLCA
    * TNP-NLA
    * TNP-NLD
    * 
    * NTP-LX
    * NTP-LT
    * NTP-LCA
    * NTP-LA
    * NTP-LD
    * 
    * NTP-NLX
    * NTP-NLT
    * NTP-NLCA
    * NTP-NLA
    * NTP-NLD
    * 
    * NTNP-LX
    * NTNP-LT
    * NTNP-LCA
    * NTNP-LA
    * NTNP-LD
    * 
    * NTNP-NLX
    * NTNP-NLT
    * NTNP-NLCA
    * NTNP-NLA
    * NTNP-NLD
    * 
    * XP-LX
    * XP-LT
    * XP-LCA
    * XP-LA
    * XP-LD
    * 
    * XP-NLX
    * XP-NLT
    * XP-NLCA
    * XP-NLA
    * XP-NLD
    * 
    * XNP-LX
    * XNP-LT
    * XNP-LCA
    * XNP-LA
    * XNP-LD
    * 
    * XNP-NLX
    * XNP-NLT
    * XNP-NLCA
    * XNP-NLA
    * XNP-NLD
    * 
    * 
    *
    *
    */
   
   //
   // Simple Queue tests
   // We test one sender and one receiver on one queue concurrently
   // with each permutation of the sender and receiver types
   //
   
   public void test_Simple_TP_LX() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      XASession sessReceive = ((XAConnection)conn2).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Transactional2PCReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 20, 3, true) };
      
      runRunners(runners);
      
      conn1.close();
      conn2.close();
   }
   
   public void test_Simple_TP_LT() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TP_LCA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new RecoveringReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TP_LA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TP_LD() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   
   public void test_Simple_TP_NLX() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      XASession sessReceive = ((XAConnection)conn2).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Transactional2PCReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   public void test_Simple_TP_NLT() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TP_NLCA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new RecoveringReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TP_NLA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TP_NLD() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TNP_LX() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      XASession sessReceive = ((XAConnection)conn2).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Transactional2PCReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TNP_LT() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TNP_LCA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new RecoveringReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TNP_LA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TNP_LD() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   
   public void test_Simple_TNP_NLX() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      XASession sessReceive = ((XAConnection)conn2).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Transactional2PCReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TNP_NLT() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TNP_NLCA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new RecoveringReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TNP_NLA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_TNP_NLD() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTP_LX() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      XASession sessReceive = ((XAConnection)conn2).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
            new Transactional2PCReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTP_LT() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
            new TransactionalReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTP_LCA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
            new RecoveringReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTP_LA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
            new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTP_LD() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
            new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   
   public void test_Simple_NTP_NLX() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      XASession sessReceive = ((XAConnection)conn2).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
            new Transactional2PCReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTP_NLT() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
            new TransactionalReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTP_NLCA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
            new RecoveringReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTP_NLA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
            new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTP_NLD() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
            new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   
   public void test_Simple_NTNP_LX() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      XASession sessReceive = ((XAConnection)conn2).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
            new Transactional2PCReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 0, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTNP_LT() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
            new TransactionalReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 0, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTNP_LCA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
            new RecoveringReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTNP_LA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
            new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTNP_LD() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
            new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   
   public void test_Simple_NTNP_NLX() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      XASession sessReceive = ((XAConnection)conn2).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
            new Transactional2PCReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTNP_NLT() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
            new TransactionalReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTNP_NLCA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
            new RecoveringReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTNP_NLA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
            new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_NTNP_NLD() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      Session sessSend = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
            new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XP_LX() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      XASession sessReceive = ((XAConnection)conn2).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Transactional2PCReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XP_LT() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XP_LCA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new RecoveringReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XP_LA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XP_LD() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   
   public void test_Simple_XP_NLX() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      XASession sessReceive = ((XAConnection)conn2).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Transactional2PCReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XP_NLT() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XP_NLCA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new RecoveringReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XP_NLA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XP_NLD() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   
   public void test_Simple_XNP_LX() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      XASession sessReceive = ((XAConnection)conn2).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Transactional2PCReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XNP_LT() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XNP_LCA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new RecoveringReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XNP_LA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XNP_LD() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, true) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XNP_NLX() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      XASession sessReceive = ((XAConnection)conn2).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Transactional2PCReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   
   public void test_Simple_XNP_NLT() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XNP_NLCA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new RecoveringReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XNP_NLA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   public void test_Simple_XNP_NLD() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      conn2.start();
      
      XASession sessSend = ((XAConnection)conn1).createXASession();
      Session sessReceive = conn2.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, false) };
      
      runRunners(runners);
      
      conn1.close(); conn2.close();      
   }
   
   
   
   
   
   /*
    * Multiple sender tests on a Queue
    * We take one of each permutation of sender and couple it with a single receiver of each receiver permutation
    * 
    * LX
    * LT
    * LCA
    * LA
    * LD
    * NLX
    * NLT
    * NLCA
    * NLA
    * NLD
    */
   
   
   
   public void test_Multiple_LX() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();
      Connection conn4 = cf.createConnection();
      Connection conn5 = cf.createConnection();
      Connection conn6 = cf.createConnection();
      Connection conn7 = cf.createConnection();
      
      conn7.start();
      
      XASession sessXP = ((XAConnection)conn1).createXASession(); 
      XASession sessXNP = ((XAConnection)conn2).createXASession();
      Session sessTP = conn3.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP = conn4.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP = conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP = conn6.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prodXP = sessXP.createProducer(queue1);
      prodXP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP = sessXNP.createProducer(queue1);
      prodXNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);      
      MessageProducer prodTP = sessTP.createProducer(queue1);
      prodTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP = sessTNP.createProducer(queue1);
      prodTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP = sessNTP.createProducer(queue1);
      prodNTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP = sessNTNP.createProducer(queue1);
      prodNTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      
      XASession sessReceive = ((XAConnection)conn7).createXASession();      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      
      Runner[] runners = new Runner[] {             
            new Transactional2PCSender("prod1", sessXP, prodXP, NUM_PERSISTENT_MESSAGES, 100, 33), 
            new Transactional2PCSender("prod2", sessXNP, prodXNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod3", sessTP, prodTP, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod4", sessTNP, prodTNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Sender("prod5", sessNTP, prodNTP, NUM_PERSISTENT_MESSAGES),
            new Sender("prod6", sessNTNP, prodNTNP, NUM_NON_PERSISTENT_MESSAGES),            
            new Transactional2PCReceiver(sessReceive, cons, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 33, true)
      };
      
      runRunners(runners);
      
      conn1.close();conn2.close();conn3.close();conn4.close();conn5.close();conn6.close();conn7.close();      
   }
   
   public void test_Multiple_LT() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();
      Connection conn4 = cf.createConnection();
      Connection conn5 = cf.createConnection();
      Connection conn6 = cf.createConnection();
      Connection conn7 = cf.createConnection();
      
      conn7.start();
      
      XASession sessXP = ((XAConnection)conn1).createXASession(); 
      XASession sessXNP = ((XAConnection)conn2).createXASession();
      Session sessTP = conn3.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP = conn4.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP = conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP = conn6.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      
      MessageProducer prodXP = sessXP.createProducer(queue1);
      prodXP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP = sessXNP.createProducer(queue1);
      prodXNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);      
      MessageProducer prodTP = sessTP.createProducer(queue1);
      prodTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP = sessTNP.createProducer(queue1);
      prodTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP = sessNTP.createProducer(queue1);
      prodNTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP = sessNTNP.createProducer(queue1);
      prodNTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      
      
      Session sessReceive = conn7.createSession(true, Session.SESSION_TRANSACTED);      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      
      Runner[] runners = new Runner[] {             
            new Transactional2PCSender("prod1", sessXP, prodXP, NUM_PERSISTENT_MESSAGES, 100, 33), 
            new Transactional2PCSender("prod2", sessXNP, prodXNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod3", sessTP, prodTP, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod4", sessTNP, prodTNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Sender("prod5", sessNTP, prodNTP, NUM_PERSISTENT_MESSAGES),
            new Sender("prod6", sessNTNP, prodNTNP, NUM_NON_PERSISTENT_MESSAGES),              
            new TransactionalReceiver(sessReceive, cons, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 33, true)
      };
      
      runRunners(runners);
      
      conn1.close();conn2.close();conn3.close();conn4.close();conn5.close();conn6.close();conn7.close();      
   }
   
   public void test_Multiple_LCA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();
      Connection conn4 = cf.createConnection();
      Connection conn5 = cf.createConnection();
      Connection conn6 = cf.createConnection();
      Connection conn7 = cf.createConnection();
      
      conn7.start();
      
      XASession sessXP = ((XAConnection)conn1).createXASession(); 
      XASession sessXNP = ((XAConnection)conn2).createXASession();
      Session sessTP = conn3.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP = conn4.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP = conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP = conn6.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prodXP = sessXP.createProducer(queue1);
      prodXP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP = sessXNP.createProducer(queue1);
      prodXNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);      
      MessageProducer prodTP = sessTP.createProducer(queue1);
      prodTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP = sessTNP.createProducer(queue1);
      prodTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP = sessNTP.createProducer(queue1);
      prodNTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP = sessNTNP.createProducer(queue1);
      prodNTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      
      
      Session sessReceive = conn7.createSession(false, Session.CLIENT_ACKNOWLEDGE);      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      
      Runner[] runners = new Runner[] {             
            new Transactional2PCSender("prod1", sessXP, prodXP, NUM_PERSISTENT_MESSAGES, 100, 33), 
            new Transactional2PCSender("prod2", sessXNP, prodXNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod3", sessTP, prodTP, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod4", sessTNP, prodTNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Sender("prod5", sessNTP, prodNTP, NUM_PERSISTENT_MESSAGES),
            new Sender("prod6", sessNTNP, prodNTNP, NUM_NON_PERSISTENT_MESSAGES),             
            new RecoveringReceiver(sessReceive, cons, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 33, true)
      };
      
      runRunners(runners);
      
      conn1.close();conn2.close();conn3.close();conn4.close();conn5.close();conn6.close();conn7.close();      
   }
   
   public void test_Multiple_LA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();
      Connection conn4 = cf.createConnection();
      Connection conn5 = cf.createConnection();
      Connection conn6 = cf.createConnection();
      Connection conn7 = cf.createConnection();
      
      conn7.start();
      
      XASession sessXP = ((XAConnection)conn1).createXASession(); 
      XASession sessXNP = ((XAConnection)conn2).createXASession();
      Session sessTP = conn3.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP = conn4.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP = conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP = conn6.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prodXP = sessXP.createProducer(queue1);
      prodXP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP = sessXNP.createProducer(queue1);
      prodXNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);      
      MessageProducer prodTP = sessTP.createProducer(queue1);
      prodTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP = sessTNP.createProducer(queue1);
      prodTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP = sessNTP.createProducer(queue1);
      prodNTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP = sessNTNP.createProducer(queue1);
      prodNTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      
      
      Session sessReceive = conn7.createSession(false, Session.AUTO_ACKNOWLEDGE);      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      
      Runner[] runners = new Runner[] {             
            new Transactional2PCSender("prod1", sessXP, prodXP, NUM_PERSISTENT_MESSAGES, 100, 33), 
            new Transactional2PCSender("prod2", sessXNP, prodXNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod3", sessTP, prodTP, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod4", sessTNP, prodTNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Sender("prod5", sessNTP, prodNTP, NUM_PERSISTENT_MESSAGES),
            new Sender("prod6", sessNTNP, prodNTNP, NUM_NON_PERSISTENT_MESSAGES),             
            new Receiver(sessReceive, cons, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, true)
      };
      
      runRunners(runners);
      
      conn1.close();conn2.close();conn3.close();conn4.close();conn5.close();conn6.close();conn7.close();      
   }
   
   public void test_Multiple_LD() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();
      Connection conn4 = cf.createConnection();
      Connection conn5 = cf.createConnection();
      Connection conn6 = cf.createConnection();
      Connection conn7 = cf.createConnection();
      
      conn7.start();
      
      XASession sessXP = ((XAConnection)conn1).createXASession(); 
      XASession sessXNP = ((XAConnection)conn2).createXASession();
      Session sessTP = conn3.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP = conn4.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP = conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP = conn6.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prodXP = sessXP.createProducer(queue1);
      prodXP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP = sessXNP.createProducer(queue1);
      prodXNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);      
      MessageProducer prodTP = sessTP.createProducer(queue1);
      prodTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP = sessTNP.createProducer(queue1);
      prodTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP = sessNTP.createProducer(queue1);
      prodNTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP = sessNTNP.createProducer(queue1);
      prodNTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      
      
      Session sessReceive = conn7.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      
      Runner[] runners = new Runner[] {             
            new Transactional2PCSender("prod1", sessXP, prodXP, NUM_PERSISTENT_MESSAGES, 100, 33), 
            new Transactional2PCSender("prod2", sessXNP, prodXNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod3", sessTP, prodTP, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod4", sessTNP, prodTNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Sender("prod5", sessNTP, prodNTP, NUM_PERSISTENT_MESSAGES),
            new Sender("prod6", sessNTNP, prodNTNP, NUM_NON_PERSISTENT_MESSAGES),             
            new Receiver(sessReceive, cons, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, true)
      };
      
      runRunners(runners);
      
      conn1.close();conn2.close();conn3.close();conn4.close();conn5.close();conn6.close();conn7.close();      
   }
   
   public void test_Multiple_NLX() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();
      Connection conn4 = cf.createConnection();
      Connection conn5 = cf.createConnection();
      Connection conn6 = cf.createConnection();
      Connection conn7 = cf.createConnection();
      
      conn7.start();
      
      XASession sessXP = ((XAConnection)conn1).createXASession(); 
      XASession sessXNP = ((XAConnection)conn2).createXASession();
      Session sessTP = conn3.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP = conn4.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP = conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP = conn6.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prodXP = sessXP.createProducer(queue1);
      prodXP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP = sessXNP.createProducer(queue1);
      prodXNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);      
      MessageProducer prodTP = sessTP.createProducer(queue1);
      prodTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP = sessTNP.createProducer(queue1);
      prodTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP = sessNTP.createProducer(queue1);
      prodNTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP = sessNTNP.createProducer(queue1);
      prodNTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      
      
      XASession sessReceive = ((XAConnection)conn7).createXASession();      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      
      Runner[] runners = new Runner[] {             
            new Transactional2PCSender("prod1", sessXP, prodXP, NUM_PERSISTENT_MESSAGES, 100, 33), 
            new Transactional2PCSender("prod2", sessXNP, prodXNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod3", sessTP, prodTP, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod4", sessTNP, prodTNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Sender("prod5", sessNTP, prodNTP, NUM_PERSISTENT_MESSAGES),
            new Sender("prod6", sessNTNP, prodNTNP, NUM_NON_PERSISTENT_MESSAGES),            
            new Transactional2PCReceiver(sessReceive, cons, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 33, false)
      };
      
      runRunners(runners);
      
      conn1.close();conn2.close();conn3.close();conn4.close();conn5.close();conn6.close();conn7.close();      
   }
   
   public void test_Multiple_NLT() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();
      Connection conn4 = cf.createConnection();
      Connection conn5 = cf.createConnection();
      Connection conn6 = cf.createConnection();
      Connection conn7 = cf.createConnection();
      
      conn7.start();
      
      XASession sessXP = ((XAConnection)conn1).createXASession(); 
      XASession sessXNP = ((XAConnection)conn2).createXASession();
      Session sessTP = conn3.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP = conn4.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP = conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP = conn6.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prodXP = sessXP.createProducer(queue1);
      prodXP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP = sessXNP.createProducer(queue1);
      prodXNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);      
      MessageProducer prodTP = sessTP.createProducer(queue1);
      prodTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP = sessTNP.createProducer(queue1);
      prodTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP = sessNTP.createProducer(queue1);
      prodNTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP = sessNTNP.createProducer(queue1);
      prodNTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      
      
      Session sessReceive = conn7.createSession(true, Session.SESSION_TRANSACTED);      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      
      Runner[] runners = new Runner[] {             
            new Transactional2PCSender("prod1", sessXP, prodXP, NUM_PERSISTENT_MESSAGES, 100, 33), 
            new Transactional2PCSender("prod2", sessXNP, prodXNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod3", sessTP, prodTP, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod4", sessTNP, prodTNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Sender("prod5", sessNTP, prodNTP, NUM_PERSISTENT_MESSAGES),
            new Sender("prod6", sessNTNP, prodNTNP, NUM_NON_PERSISTENT_MESSAGES),            
            new TransactionalReceiver(sessReceive, cons, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 33, false)
      };
      
      runRunners(runners);
      
      conn1.close();conn2.close();conn3.close();conn4.close();conn5.close();conn6.close();conn7.close();      
   }
   
   public void test_Multiple_NLCA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();
      Connection conn4 = cf.createConnection();
      Connection conn5 = cf.createConnection();
      Connection conn6 = cf.createConnection();
      Connection conn7 = cf.createConnection();
      
      conn7.start();
      
      XASession sessXP = ((XAConnection)conn1).createXASession(); 
      XASession sessXNP = ((XAConnection)conn2).createXASession();
      Session sessTP = conn3.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP = conn4.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP = conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP = conn6.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prodXP = sessXP.createProducer(queue1);
      prodXP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP = sessXNP.createProducer(queue1);
      prodXNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);      
      MessageProducer prodTP = sessTP.createProducer(queue1);
      prodTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP = sessTNP.createProducer(queue1);
      prodTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP = sessNTP.createProducer(queue1);
      prodNTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP = sessNTNP.createProducer(queue1);
      prodNTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      
      
      Session sessReceive = conn7.createSession(false, Session.CLIENT_ACKNOWLEDGE);      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      
      Runner[] runners = new Runner[] {             
            new Transactional2PCSender("prod1", sessXP, prodXP, NUM_PERSISTENT_MESSAGES, 100, 33), 
            new Transactional2PCSender("prod2", sessXNP, prodXNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod3", sessTP, prodTP, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod4", sessTNP, prodTNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Sender("prod5", sessNTP, prodNTP, NUM_PERSISTENT_MESSAGES),
            new Sender("prod6", sessNTNP, prodNTNP, NUM_NON_PERSISTENT_MESSAGES),             
            new RecoveringReceiver(sessReceive, cons, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 33, false)
      };
      
      runRunners(runners);
      
      conn1.close();conn2.close();conn3.close();conn4.close();conn5.close();conn6.close();conn7.close();      
   }
   
   public void test_Multiple_NLA() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();
      Connection conn4 = cf.createConnection();
      Connection conn5 = cf.createConnection();
      Connection conn6 = cf.createConnection();
      Connection conn7 = cf.createConnection();
      
      conn7.start();
      
      XASession sessXP = ((XAConnection)conn1).createXASession(); 
      XASession sessXNP = ((XAConnection)conn2).createXASession();
      Session sessTP = conn3.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP = conn4.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP = conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP = conn6.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prodXP = sessXP.createProducer(queue1);
      prodXP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP = sessXNP.createProducer(queue1);
      prodXNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);      
      MessageProducer prodTP = sessTP.createProducer(queue1);
      prodTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP = sessTNP.createProducer(queue1);
      prodTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP = sessNTP.createProducer(queue1);
      prodNTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP = sessNTNP.createProducer(queue1);
      prodNTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      
      
      Session sessReceive = conn7.createSession(false, Session.AUTO_ACKNOWLEDGE);      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      
      Runner[] runners = new Runner[] {             
            new Transactional2PCSender("prod1", sessXP, prodXP, NUM_PERSISTENT_MESSAGES, 100, 33), 
            new Transactional2PCSender("prod2", sessXNP, prodXNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod3", sessTP, prodTP, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod4", sessTNP, prodTNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Sender("prod5", sessNTP, prodNTP, NUM_PERSISTENT_MESSAGES),
            new Sender("prod6", sessNTNP, prodNTNP, NUM_NON_PERSISTENT_MESSAGES),             
            new Receiver(sessReceive, cons, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, false)
      };
      
      runRunners(runners);
      
      conn1.close();conn2.close();conn3.close();conn4.close();conn5.close();conn6.close();conn7.close();      
   }   
   
   public void test_Multiple_NLD() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();
      Connection conn4 = cf.createConnection();
      Connection conn5 = cf.createConnection();
      Connection conn6 = cf.createConnection();
      Connection conn7 = cf.createConnection();
      
      conn7.start();
      
      XASession sessXP = ((XAConnection)conn1).createXASession(); 
      XASession sessXNP = ((XAConnection)conn2).createXASession();
      Session sessTP = conn3.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP = conn4.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP = conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP = conn6.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prodXP = sessXP.createProducer(queue1);
      prodXP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP = sessXNP.createProducer(queue1);
      prodXNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);      
      MessageProducer prodTP = sessTP.createProducer(queue1);
      prodTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP = sessTNP.createProducer(queue1);
      prodTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP = sessNTP.createProducer(queue1);
      prodNTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP = sessNTNP.createProducer(queue1);
      prodNTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      
      
      Session sessReceive = conn7.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      
      Runner[] runners = new Runner[] {             
            new Transactional2PCSender("prod1", sessXP, prodXP, NUM_PERSISTENT_MESSAGES, 100, 33), 
            new Transactional2PCSender("prod2", sessXNP, prodXNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod3", sessTP, prodTP, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod4", sessTNP, prodTNP, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Sender("prod5", sessNTP, prodNTP, NUM_PERSISTENT_MESSAGES),
            new Sender("prod6", sessNTNP, prodNTNP, NUM_NON_PERSISTENT_MESSAGES),            
            new Receiver(sessReceive, cons, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, false)
      };
      
      runRunners(runners);
      
      conn1.close();conn2.close();conn3.close();conn4.close();conn5.close();conn6.close();conn7.close();      
   }   
   
   
   
   
   /*
    * The next test hammers a single queue with multiple transactional senders with different transactions sizes
    * Half of them send persistent messages, the other half non persistent messages
    */
   public void test_Multiple_Tx_Senders() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();
      Connection conn4 = cf.createConnection();
      Connection conn5 = cf.createConnection();
      Connection conn6 = cf.createConnection();
      Connection conn7 = cf.createConnection();
      Connection conn8 = cf.createConnection();
      Connection conn9 = cf.createConnection();
      Connection conn10 = cf.createConnection();
      Connection conn11 = cf.createConnection();
      Connection conn12 = cf.createConnection();
      Connection conn13 = cf.createConnection();
      Connection conn14 = cf.createConnection();
      Connection conn15 = cf.createConnection();
      Connection conn16 = cf.createConnection();
      Connection conn17 = cf.createConnection();

      conn17.start();
      
      Session sess1 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      Session sess2 = conn2.createSession(true, Session.SESSION_TRANSACTED);
      Session sess3 = conn3.createSession(true, Session.SESSION_TRANSACTED);
      Session sess4 = conn4.createSession(true, Session.SESSION_TRANSACTED);
      Session sess5 = conn5.createSession(true, Session.SESSION_TRANSACTED);
      Session sess6 = conn6.createSession(true, Session.SESSION_TRANSACTED);
      Session sess7 = conn7.createSession(true, Session.SESSION_TRANSACTED);
      Session sess8 = conn8.createSession(true, Session.SESSION_TRANSACTED);
      
      XASession sess9 = ((XAConnection)conn9).createXASession();
      XASession sess10 = ((XAConnection)conn10).createXASession();
      XASession sess11 = ((XAConnection)conn11).createXASession();
      XASession sess12 = ((XAConnection)conn12).createXASession();
      XASession sess13 = ((XAConnection)conn13).createXASession();
      XASession sess14 = ((XAConnection)conn14).createXASession();
      XASession sess15 = ((XAConnection)conn15).createXASession();
      XASession sess16 = ((XAConnection)conn16).createXASession();
      
      
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
      
      
      Session sessReceive = conn17.createSession(false, Session.AUTO_ACKNOWLEDGE);      
      MessageConsumer cons = sessReceive.createConsumer(queue1);
      
      Runner[] runners = 
         new Runner[] {
            new TransactionalSender("prod1", sess1, prod1, NUM_NON_PERSISTENT_MESSAGES, 1, 1),
            new TransactionalSender("prod2", sess2, prod2, NUM_PERSISTENT_MESSAGES, 1, 1),
            new TransactionalSender("prod3", sess3, prod3, NUM_NON_PERSISTENT_MESSAGES, 10, 7),
            new TransactionalSender("prod4", sess4, prod4, NUM_PERSISTENT_MESSAGES, 10, 7),
            new TransactionalSender("prod5", sess5, prod5, NUM_NON_PERSISTENT_MESSAGES, 50, 21),
            new TransactionalSender("prod6", sess6, prod6, NUM_PERSISTENT_MESSAGES, 50, 21),
            new TransactionalSender("prod7", sess7, prod7, NUM_NON_PERSISTENT_MESSAGES, 100, 67),
            new TransactionalSender("prod8", sess8, prod8, NUM_PERSISTENT_MESSAGES, 100, 67),            
            new Transactional2PCSender("prod9", sess9, prod9, NUM_NON_PERSISTENT_MESSAGES, 1, 1),
            new Transactional2PCSender("prod10", sess10, prod10, NUM_PERSISTENT_MESSAGES, 1, 1),
            new Transactional2PCSender("prod11", sess11, prod11, NUM_NON_PERSISTENT_MESSAGES, 10, 7),
            new Transactional2PCSender("prod12", sess12, prod12, NUM_PERSISTENT_MESSAGES, 10, 7),
            new Transactional2PCSender("prod13", sess13, prod13, NUM_NON_PERSISTENT_MESSAGES, 50, 21),
            new Transactional2PCSender("prod14", sess14, prod14, NUM_PERSISTENT_MESSAGES, 50, 21),
            new Transactional2PCSender("prod15", sess15, prod15, NUM_NON_PERSISTENT_MESSAGES, 100, 67),
            new Transactional2PCSender("prod16", sess16, prod16, NUM_PERSISTENT_MESSAGES, 100, 67),            
            new Receiver(sessReceive, cons, 8 * NUM_PERSISTENT_MESSAGES + 8 * NUM_NON_PERSISTENT_MESSAGES, false)
      };
      
      runRunners(runners);
      
      conn1.close();
      conn2.close();
      conn3.close();
      conn4.close();
      conn5.close();
      conn6.close();
      conn7.close();
      conn8.close();
      conn9.close();
      conn10.close();
      conn11.close();
      conn12.close();
      conn13.close();
      conn14.close();
      conn15.close();
      conn16.close();
      conn17.close();
      
   }   
   
   
   /*
    * Now we try sending with multiple senders concurrently to multiple different queues
    */
   public void test_Multiple_MultipleQueues() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();
      Connection conn4 = cf.createConnection();
      Connection conn5 = cf.createConnection();
      Connection conn6 = cf.createConnection();
      Connection conn7 = cf.createConnection();
      Connection conn8 = cf.createConnection();
      Connection conn9 = cf.createConnection();
      Connection conn10 = cf.createConnection();
      Connection conn11 = cf.createConnection();
      Connection conn12 = cf.createConnection();
      Connection conn13 = cf.createConnection();
      Connection conn14 = cf.createConnection();
      Connection conn15 = cf.createConnection();
      Connection conn16 = cf.createConnection();
      Connection conn17 = cf.createConnection();
      Connection conn18 = cf.createConnection();
      Connection conn19 = cf.createConnection();
      Connection conn20 = cf.createConnection();
      Connection conn21 = cf.createConnection();
      Connection conn22 = cf.createConnection();
      Connection conn23 = cf.createConnection();
      Connection conn24 = cf.createConnection();
      Connection conn25 = cf.createConnection();
      Connection conn26 = cf.createConnection();
      Connection conn27 = cf.createConnection();
      Connection conn28 = cf.createConnection();
            
      conn25.start();
      conn26.start();
      conn27.start();
      conn28.start();
            
      XASession sessXP1 = ((XAConnection)conn1).createXASession();
      XASession sessXNP1 = ((XAConnection)conn2).createXASession();
      Session sessTP1 = conn3.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP1 = conn4.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP1 = conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP1 = conn6.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prodXP1 = sessXP1.createProducer(queue1);
      prodXP1.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP1 = sessXNP1.createProducer(queue1);
      prodXNP1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodTP1 = sessTP1.createProducer(queue1);
      prodTP1.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP1 = sessTNP1.createProducer(queue1);
      prodTNP1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP1 = sessNTP1.createProducer(queue1);
      prodNTP1.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP1 = sessNTNP1.createProducer(queue1);
      prodNTNP1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      XASession sessXP2 = ((XAConnection)conn7).createXASession();
      XASession sessXNP2 = ((XAConnection)conn8).createXASession();
      Session sessTP2 = conn9.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP2 = conn10.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP2 = conn11.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP2 = conn12.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prodXP2 = sessXP2.createProducer(queue2);
      prodXP2.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP2 = sessXNP2.createProducer(queue2);
      prodXNP2.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodTP2 = sessTP2.createProducer(queue2);
      prodTP2.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP2 = sessTNP2.createProducer(queue2);
      prodTNP2.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP2 = sessNTP2.createProducer(queue2);
      prodNTP2.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP2 = sessNTNP2.createProducer(queue2);
      prodNTNP2.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      XASession sessXP3 = ((XAConnection)conn13).createXASession();
      XASession sessXNP3 = ((XAConnection)conn14).createXASession();
      Session sessTP3 = conn15.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP3 = conn16.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP3 = conn17.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP3 = conn18.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prodXP3 = sessXP3.createProducer(queue3);
      prodXP3.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP3 = sessXNP3.createProducer(queue3);
      prodXNP3.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodTP3 = sessTP3.createProducer(queue3);
      prodTP3.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP3 = sessTNP3.createProducer(queue3);
      prodTNP3.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP3 = sessNTP3.createProducer(queue3);
      prodNTP3.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP3 = sessNTNP3.createProducer(queue3);
      prodNTNP3.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      XASession sessXP4 = ((XAConnection)conn19).createXASession();
      XASession sessXNP4 = ((XAConnection)conn20).createXASession();
      Session sessTP4 = conn21.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP4 = conn22.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP4 = conn23.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP4 = conn24.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prodXP4 = sessXP4.createProducer(queue4);
      prodXP4.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP4 = sessXNP4.createProducer(queue4);
      prodXNP4.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodTP4 = sessTP4.createProducer(queue4);
      prodTP4.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP4 = sessTNP4.createProducer(queue4);
      prodTNP4.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP4 = sessNTP4.createProducer(queue4);
      prodNTP4.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP4 = sessNTNP4.createProducer(queue4);
      prodNTNP4.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      
      Session sessReceive1 = conn25.createSession(false, Session.AUTO_ACKNOWLEDGE);      
      MessageConsumer cons1 = sessReceive1.createConsumer(queue1);
      Session sessReceive2 = conn26.createSession(false, Session.AUTO_ACKNOWLEDGE);      
      MessageConsumer cons2 = sessReceive2.createConsumer(queue2);
      Session sessReceive3 = conn27.createSession(false, Session.AUTO_ACKNOWLEDGE);      
      MessageConsumer cons3 = sessReceive3.createConsumer(queue3);
      Session sessReceive4 = conn28.createSession(false, Session.AUTO_ACKNOWLEDGE);      
      MessageConsumer cons4 = sessReceive4.createConsumer(queue4);
      
      Runner[] runners = new Runner[] 
                                    {             
            new Transactional2PCSender("prod1_1", sessXP1, prodXP1, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Transactional2PCSender("prod1_2", sessXNP1, prodXNP1, NUM_NON_PERSISTENT_MESSAGES, 100, 33),            
            new TransactionalSender("prod1_3", sessTP1, prodTP1, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod1_4", sessTNP1, prodTNP1, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Sender("prod1_5", sessNTP1, prodNTP1, NUM_PERSISTENT_MESSAGES),
            new Sender("prod1_6", sessNTNP1, prodNTNP1, NUM_NON_PERSISTENT_MESSAGES),                      
            
            new Transactional2PCSender("prod2_1", sessXP2, prodXP2, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Transactional2PCSender("prod2_2", sessXNP2, prodXNP2, NUM_NON_PERSISTENT_MESSAGES, 100, 33), 
            new TransactionalSender("prod2_3", sessTP2, prodTP2, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod2_4", sessTNP2, prodTNP2, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Sender("prod2_5", sessNTP2, prodNTP2, NUM_PERSISTENT_MESSAGES),
            new Sender("prod2_6", sessNTNP2, prodNTNP2, NUM_NON_PERSISTENT_MESSAGES),                
            
            new Transactional2PCSender("prod3_1", sessXP3, prodXP3, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Transactional2PCSender("prod3_2", sessXNP3, prodXNP3, NUM_NON_PERSISTENT_MESSAGES, 100, 33), 
            new TransactionalSender("prod3_3", sessTP3, prodTP3, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod3_4", sessTNP3, prodTNP3, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Sender("prod3_5", sessNTP3, prodNTP3, NUM_PERSISTENT_MESSAGES),
            new Sender("prod3_6", sessNTNP3, prodNTNP3, NUM_NON_PERSISTENT_MESSAGES),
            
            new Transactional2PCSender("prod4_1", sessXP4, prodXP4, NUM_PERSISTENT_MESSAGES, 100, 33),
            new Transactional2PCSender("prod4_2", sessXNP4, prodXNP4, NUM_NON_PERSISTENT_MESSAGES, 100, 33), 
            new TransactionalSender("prod4_3", sessTP4, prodTP4, NUM_PERSISTENT_MESSAGES, 100, 33),
            new TransactionalSender("prod4_4", sessTNP4, prodTNP4, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
            new Sender("prod4_5", sessNTP4, prodNTP4, NUM_PERSISTENT_MESSAGES),
            new Sender("prod4_6", sessNTNP4, prodNTNP4, NUM_NON_PERSISTENT_MESSAGES),
            
            new Receiver(sessReceive1, cons1, 2 * NUM_PERSISTENT_MESSAGES + 2 * NUM_NON_PERSISTENT_MESSAGES, false),
            new Receiver(sessReceive2, cons2, 2 * NUM_PERSISTENT_MESSAGES + 2 * NUM_NON_PERSISTENT_MESSAGES, false),
            new Receiver(sessReceive3, cons3, 2 * NUM_PERSISTENT_MESSAGES + 2 * NUM_NON_PERSISTENT_MESSAGES, false),
            new Receiver(sessReceive4, cons4, 2 * NUM_PERSISTENT_MESSAGES + 2 * NUM_NON_PERSISTENT_MESSAGES, false),
                                    };
      
      runRunners(runners);
      
      conn1.close();
      conn2.close();
      conn3.close();
      conn4.close();
      conn5.close();
      conn6.close();
      conn7.close();
      conn8.close();
      conn9.close();
      conn10.close();
      conn11.close();
      conn12.close();
      conn13.close();
      conn14.close();
      conn15.close();
      conn16.close();
      conn17.close();
      conn18.close();
      conn19.close();
      conn20.close();
      conn21.close();
      conn22.close();
      conn23.close();
      conn24.close();
   }   
   
}
