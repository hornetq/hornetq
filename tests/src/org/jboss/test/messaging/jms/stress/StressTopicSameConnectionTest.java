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
 * Stress tests for JMS topics using the same connection for all sessions in a test  
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * StressTopicSameConnection.java,v 1.1 2006/01/17 12:15:33 timfox Exp
 */
public class StressTopicSameConnectionTest extends StressTestBase
{

   public StressTopicSameConnectionTest(String name)
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
   // Simple Topic tests
   // We test one sender and one receiver on one topic concurrently
   // with each permutation of the sender and receiver types
   //
   
   public void test_Simple_TP_LX() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      XASession sessReceive = ((XAConnection)conn).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new Transactional2PCReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 20, 3, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TP_LT() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new TransactionalReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TP_LCA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new RecoveringReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TP_LA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TP_LD() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, true) };

      runRunners(runners);

      conn.close();      
   }
   
   
   public void test_Simple_TP_NLX() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      XASession sessReceive = ((XAConnection)conn).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new Transactional2PCReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   public void test_Simple_TP_NLT() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new TransactionalReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TP_NLCA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new RecoveringReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TP_NLA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TP_NLD() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TNP_LX() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      XASession sessReceive = ((XAConnection)conn).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new Transactional2PCReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
 
   public void test_Simple_TNP_LT() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new TransactionalReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TNP_LCA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new RecoveringReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TNP_LA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TNP_LD() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, true) };

      runRunners(runners);

      conn.close();      
   }
   
   
   public void test_Simple_TNP_NLX() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      XASession sessReceive = ((XAConnection)conn).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new Transactional2PCReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TNP_NLT() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new TransactionalReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TNP_NLCA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new RecoveringReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TNP_NLA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_TNP_NLD() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessReceive = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new TransactionalSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTP_LX() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      XASession sessReceive = ((XAConnection)conn).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
                                        new Transactional2PCReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTP_LT() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
                                        new TransactionalReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTP_LCA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
                                        new RecoveringReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTP_LA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
                                        new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTP_LD() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
                                        new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, true) };

      runRunners(runners);

      conn.close();      
   }
   
   
   public void test_Simple_NTP_NLX() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      XASession sessReceive = ((XAConnection)conn).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
                                        new Transactional2PCReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTP_NLT() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
                                        new TransactionalReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTP_NLCA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
                                        new RecoveringReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTP_NLA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
                                        new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTP_NLD() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
                                        new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, false) };

      runRunners(runners);

      conn.close();      
   }
   
   
   public void test_Simple_NTNP_LX() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      XASession sessReceive = ((XAConnection)conn).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
                                        new Transactional2PCReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 0, true) };

      runRunners(runners);

      conn.close();      
   }
 
   public void test_Simple_NTNP_LT() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
                                        new TransactionalReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 0, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTNP_LCA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
                                        new RecoveringReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTNP_LA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
                                        new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTNP_LD() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
                                        new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, true) };

      runRunners(runners);

      conn.close();      
   }
   
   
   public void test_Simple_NTNP_NLX() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      XASession sessReceive = ((XAConnection)conn).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
                                        new Transactional2PCReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTNP_NLT() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
                                        new TransactionalReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTNP_NLCA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
                                        new RecoveringReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTNP_NLA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
                                        new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_NTNP_NLD() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessReceive = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES),
                                        new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XP_LX() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      XASession sessReceive = ((XAConnection)conn).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new Transactional2PCReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XP_LT() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new TransactionalReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XP_LCA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new RecoveringReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XP_LA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XP_LD() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, true) };

      runRunners(runners);

      conn.close();      
   }
   
   
   public void test_Simple_XP_NLX() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      XASession sessReceive = ((XAConnection)conn).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new Transactional2PCReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XP_NLT() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new TransactionalReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XP_NLCA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new RecoveringReceiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XP_NLA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XP_NLD() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_PERSISTENT_MESSAGES, false) };

      runRunners(runners);

      conn.close();      
   }
   
 
   public void test_Simple_XNP_LX() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      XASession sessReceive = ((XAConnection)conn).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new Transactional2PCReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XNP_LT() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new TransactionalReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XNP_LCA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new RecoveringReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XNP_LA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XNP_LD() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, true) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XNP_NLX() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      XASession sessReceive = ((XAConnection)conn).createXASession();
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new Transactional2PCReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   
   public void test_Simple_XNP_NLT() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new TransactionalReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XNP_NLCA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new RecoveringReceiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, 100, 67, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XNP_NLA() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, false) };

      runRunners(runners);

      conn.close();      
   }
   
   public void test_Simple_XNP_NLD() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      XASession sessSend = ((XAConnection)conn).createXASession();
      Session sessReceive = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      MessageConsumer cons = sessReceive.createConsumer(topic1);
      MessageProducer prod = sessSend.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Transactional2PCSender("prod1", sessSend, prod, NUM_NON_PERSISTENT_MESSAGES, 100, 33),
                                        new Receiver(sessReceive, cons, NUM_NON_PERSISTENT_MESSAGES, false) };

      runRunners(runners);

      conn.close();      
   }
   

   
   
   
   /*
    * Multiple sender tests on a Queue
    * We take one of each permutation of sender and couple it with one of each receiver
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

   
   
   public void test_Multiple() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
            
      //Sending sessions
      XASession sessXP = ((XAConnection)conn).createXASession(); 
      XASession sessXNP = ((XAConnection)conn).createXASession();
      Session sessTP = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessTNP = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNTP = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNTNP = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prodXP = sessXP.createProducer(topic1);
      prodXP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodXNP = sessXNP.createProducer(topic1);
      prodXNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);      
      MessageProducer prodTP = sessTP.createProducer(topic1);
      prodTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodTNP = sessTNP.createProducer(topic1);
      prodTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prodNTP = sessNTP.createProducer(topic1);
      prodNTP.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prodNTNP = sessNTNP.createProducer(topic1);
      prodNTNP.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      //Receiving sessions
      
      XASession sessLX = ((XAConnection)conn).createXASession();
      Session sessLT = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessLCA = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      Session sessLA = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessLD = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      XASession sessNLX = ((XAConnection)conn).createXASession();
      Session sessNLT = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sessNLCA = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      Session sessNLA = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessNLD = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      
      
//      MessageConsumer consLX = sessLX.createConsumer(topic1);
//      MessageConsumer consLT = sessLT.createConsumer(topic1);
//      MessageConsumer consLCA = sessLCA.createConsumer(topic1);
//      MessageConsumer consLA = sessLA.createConsumer(topic1);
//      MessageConsumer consLD = sessLD.createConsumer(topic1);
//      
//      MessageConsumer consNLX = sessNLX.createConsumer(topic1);
//      MessageConsumer consNLT = sessNLT.createConsumer(topic1);
//      MessageConsumer consNLCA = sessNLCA.createConsumer(topic1);
//      MessageConsumer consNLA = sessNLA.createConsumer(topic1);
//      MessageConsumer consNLD = sessNLD.createConsumer(topic1);
      
      MessageConsumer consLX = sessLX.createConsumer(topic1);
      MessageConsumer consLT = sessLT.createConsumer(topic1);
      MessageConsumer consLCA = sessLCA.createConsumer(topic1);
      MessageConsumer consLA = sessLA.createConsumer(topic1);
      MessageConsumer consLD = sessLD.createConsumer(topic1);
      
      MessageConsumer consNLX = sessNLX.createConsumer(topic1);
      MessageConsumer consNLT = sessNLT.createConsumer(topic1);
      MessageConsumer consNLCA = sessNLCA.createConsumer(topic1);
      MessageConsumer consNLA = sessNLA.createConsumer(topic1);
      MessageConsumer consNLD = sessNLD.createConsumer(topic1);
      
//      Runner[] runners = new Runner[] {             
//            new Transactional2PCSender("prod1", sessXP, prodXP, NUM_PERSISTENT_MESSAGES, 100, 25), 
//            new Transactional2PCSender("prod2", sessXNP, prodXNP, NUM_NON_PERSISTENT_MESSAGES, 100, 25),
////            new TransactionalSender("prod3", sessTP, prodTP, NUM_PERSISTENT_MESSAGES, 100, 25),
////            new TransactionalSender("prod4", sessTNP, prodTNP, NUM_NON_PERSISTENT_MESSAGES, 100, 25),
////            new Sender("prod5", sessNTP, prodNTP, NUM_PERSISTENT_MESSAGES),
////            new Sender("prod6", sessNTNP, prodNTNP, NUM_NON_PERSISTENT_MESSAGES),          
//                        
////            new Transactional2PCReceiver(sessLX, consLX, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 25, true),
////            new TransactionalReceiver(sessLT, consLT, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 25, true),
////            new RecoveringReceiver(sessLCA, consLCA, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 25, true),
////            new Receiver(sessLA, consLA, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, true),
////            new Receiver(sessLD, consLD, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, true),
//            
//            new Transactional2PCReceiver(sessNLX, consNLX, NUM_PERSISTENT_MESSAGES + NUM_NON_PERSISTENT_MESSAGES, 100, 25, false)
// //          new TransactionalReceiver(sessNLT, consNLT, NUM_PERSISTENT_MESSAGES, 100, 25, false)
// //           new RecoveringReceiver(sessNLCA, consNLCA, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 25, false),
////            new Receiver(sessNLA, consNLA, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, false),
////            new Receiver(sessNLD, consNLD, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, false)
////                                   
//      };
      
      Runner[] runners = new Runner[] {             
            new Transactional2PCSender("prod1", sessXP, prodXP, NUM_PERSISTENT_MESSAGES, 100, 25), 
            new Transactional2PCSender("prod2", sessXNP, prodXNP, NUM_NON_PERSISTENT_MESSAGES, 100, 25),
            new TransactionalSender("prod3", sessTP, prodTP, NUM_PERSISTENT_MESSAGES, 100, 25),
            new TransactionalSender("prod4", sessTNP, prodTNP, NUM_NON_PERSISTENT_MESSAGES, 100, 25),
            new Sender("prod5", sessNTP, prodNTP, NUM_PERSISTENT_MESSAGES),
            new Sender("prod6", sessNTNP, prodNTNP, NUM_NON_PERSISTENT_MESSAGES),          
                        
            new Transactional2PCReceiver(sessLX, consLX, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 25, true),
            new TransactionalReceiver(sessLT, consLT, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 25, true),
            new RecoveringReceiver(sessLCA, consLCA, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 25, true),
            new Receiver(sessLA, consLA, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, true),
            new Receiver(sessLD, consLD, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, true),
            
            new Transactional2PCReceiver(sessNLX, consNLX, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 25, false),
            new TransactionalReceiver(sessNLT, consNLT, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 25, false),
            new RecoveringReceiver(sessNLCA, consNLCA, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, 100, 25, false),
            new Receiver(sessNLA, consNLA, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, false),
            new Receiver(sessNLD, consNLD, 3 * NUM_PERSISTENT_MESSAGES + 3 * NUM_NON_PERSISTENT_MESSAGES, false)
                                   
      };      
      
      runRunners(runners);
      
      conn.close();      
   }
   
   
   
   /*
    * The next test hammers a single topic with multiple transactional senders and receivers with different transactions sizes
    * Half of them send persistent messages, the other half non persistent messages
    */
   public void test_Multiple_Tx() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      
      //Sending sessions
      
      Session sess1 = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sess2 = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sess3 = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sess4 = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sess5 = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sess6 = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sess7 = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sess8 = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      XASession sess9 = ((XAConnection)conn).createXASession();
      XASession sess10 = ((XAConnection)conn).createXASession();
      XASession sess11 = ((XAConnection)conn).createXASession();
      XASession sess12 = ((XAConnection)conn).createXASession();
      XASession sess13 = ((XAConnection)conn).createXASession();
      XASession sess14 = ((XAConnection)conn).createXASession();
      XASession sess15 = ((XAConnection)conn).createXASession();
      XASession sess16 = ((XAConnection)conn).createXASession();

      
      MessageProducer prod1 = sess1.createProducer(topic1);
      prod1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod2 = sess2.createProducer(topic1);
      prod2.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod3 = sess3.createProducer(topic1);
      prod3.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod4 = sess4.createProducer(topic1);
      prod4.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod5 = sess5.createProducer(topic1);
      prod5.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod6 = sess6.createProducer(topic1);
      prod6.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod7 = sess7.createProducer(topic1);
      prod7.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod8 = sess8.createProducer(topic1);
      prod8.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      MessageProducer prod9 = sess9.createProducer(topic1);
      prod9.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod10 = sess10.createProducer(topic1);
      prod10.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod11 = sess11.createProducer(topic1);
      prod11.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod12 = sess12.createProducer(topic1);
      prod12.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod13 = sess13.createProducer(topic1);
      prod13.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod14 = sess14.createProducer(topic1);
      prod14.setDeliveryMode(DeliveryMode.PERSISTENT);
      MessageProducer prod15 = sess15.createProducer(topic1);
      prod15.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageProducer prod16 = sess16.createProducer(topic1);
      prod16.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      //Receiving sessions
      
      Session sess17 = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sess18 = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sess19 = conn.createSession(true, Session.SESSION_TRANSACTED);
      Session sess20 = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      XASession sess21= ((XAConnection)conn).createXASession();
      XASession sess22 = ((XAConnection)conn).createXASession();
      XASession sess23 = ((XAConnection)conn).createXASession();
      XASession sess24 = ((XAConnection)conn).createXASession();
      
      MessageConsumer cons1 = sess17.createConsumer(topic1);
      MessageConsumer cons2 = sess18.createConsumer(topic1);
      MessageConsumer cons3 = sess19.createConsumer(topic1);
      MessageConsumer cons4 = sess20.createConsumer(topic1);
      MessageConsumer cons5 = sess21.createConsumer(topic1);
      MessageConsumer cons6 = sess22.createConsumer(topic1);
      MessageConsumer cons7 = sess23.createConsumer(topic1);
      MessageConsumer cons8 = sess24.createConsumer(topic1);
      

      Runner[] runners = 
         new Runner[] {
            new TransactionalSender("prod1", sess1, prod1, NUM_NON_PERSISTENT_MESSAGES, 1, 1),
            new TransactionalSender("prod2", sess2, prod2, NUM_PERSISTENT_MESSAGES, 1, 1),
            new TransactionalSender("prod3", sess3, prod3, NUM_NON_PERSISTENT_MESSAGES, 10, 5),
            new TransactionalSender("prod4", sess4, prod4, NUM_PERSISTENT_MESSAGES, 10, 5),
            new TransactionalSender("prod5", sess5, prod5, NUM_NON_PERSISTENT_MESSAGES, 50, 25),
            new TransactionalSender("prod6", sess6, prod6, NUM_PERSISTENT_MESSAGES, 50, 25),
            new TransactionalSender("prod7", sess7, prod7, NUM_NON_PERSISTENT_MESSAGES, 100, 25),
            new TransactionalSender("prod8", sess8, prod8, NUM_PERSISTENT_MESSAGES, 100, 25),            
            new Transactional2PCSender("prod9", sess9, prod9, NUM_NON_PERSISTENT_MESSAGES, 1, 1),
            new Transactional2PCSender("prod10", sess10, prod10, NUM_PERSISTENT_MESSAGES, 1, 1),
            new Transactional2PCSender("prod11", sess11, prod11, NUM_NON_PERSISTENT_MESSAGES, 10, 5),
            new Transactional2PCSender("prod12", sess12, prod12, NUM_PERSISTENT_MESSAGES, 10, 5),
            new Transactional2PCSender("prod13", sess13, prod13, NUM_NON_PERSISTENT_MESSAGES, 50, 25),
            new Transactional2PCSender("prod14", sess14, prod14, NUM_PERSISTENT_MESSAGES, 50, 25),
            new Transactional2PCSender("prod15", sess15, prod15, NUM_NON_PERSISTENT_MESSAGES, 100, 25),
            new Transactional2PCSender("prod16", sess16, prod16, NUM_PERSISTENT_MESSAGES, 100, 25),            
            
            new TransactionalReceiver(sess17, cons1,  8 * NUM_PERSISTENT_MESSAGES + 8 * NUM_NON_PERSISTENT_MESSAGES, 1, 1, false),
            new TransactionalReceiver(sess18, cons2,  8 * NUM_PERSISTENT_MESSAGES + 8 * NUM_NON_PERSISTENT_MESSAGES, 10, 5, false),
            new TransactionalReceiver(sess19, cons3,  8 * NUM_PERSISTENT_MESSAGES + 8 * NUM_NON_PERSISTENT_MESSAGES, 50, 25, false),
            new TransactionalReceiver(sess20, cons4,  8 * NUM_PERSISTENT_MESSAGES + 8 * NUM_NON_PERSISTENT_MESSAGES, 100, 25, false),

            new Transactional2PCReceiver(sess21, cons5,  8 * NUM_PERSISTENT_MESSAGES + 8 * NUM_NON_PERSISTENT_MESSAGES, 1, 1, false),
            new Transactional2PCReceiver(sess22, cons6,  8 * NUM_PERSISTENT_MESSAGES + 8 * NUM_NON_PERSISTENT_MESSAGES, 10, 5, false),
            new Transactional2PCReceiver(sess23, cons7,  8 * NUM_PERSISTENT_MESSAGES + 8 * NUM_NON_PERSISTENT_MESSAGES, 50, 25, false),
            new Transactional2PCReceiver(sess24, cons8,  8 * NUM_PERSISTENT_MESSAGES + 8 * NUM_NON_PERSISTENT_MESSAGES, 100, 25, false)
                       };

      runRunners(runners);

      conn.close();      
   }   

}
   
   
   
   
   
   
   
   
   
   
   
