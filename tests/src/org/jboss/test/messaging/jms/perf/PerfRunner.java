/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import javax.jms.DeliveryMode;
import javax.jms.Session;

import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class PerfRunner
{
   private static final Logger log = Logger.getLogger(PerfRunner.class);   
   
   
   public static void main(String[] args)
   {
      new PerfRunner().run();
   }
   
   private PerfRunner()
   {     
   }
   
   protected String defaultJMSServerURL = "jnp://localhost:1099";
   
   protected String testQueueName = "queue/testQueue";
   
   protected String testTopicName = "topic/testTopic";
   
   protected String testSubscriptionName = "perfTestSub";
   
   protected String defaultSlaveURL = "socket://localhost:1234/?socketTimeout=0";
   
   public void run()
   {
      log.info("Starting JBoss JMS performance test suite");
      
      
      
   }
   
   /*
    * Send 10000 non-persistent messages of 1024 bytes each to single queue non transactionally.
    * The queue has no consumers.
    * Measure time taken
    */
   public void testQueue1()
   {
      log.info("Running test Queue1");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      SenderJob sender = createDefaultSenderJob(testQueueName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(false);
      sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      long result = runJob(sender).getTestTime();
      
      storeResult("Queue1", result);
      
      log.info("Test Queue1 finished");
   }
   
   /*
    * Send 10000 persistent messages of 1024 bytes each to single queue non transactionally.
    * The queue has no consumers.
    * Measure time taken
    */
   public void testQueue2()
   {
      log.info("Running test Queue2");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      SenderJob sender = createDefaultSenderJob(testQueueName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(false);
      sender.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      long result = runJob(sender).getTestTime();
      
      storeResult("Queue2", result);
      
      log.info("Test Queue1 finished");
   }
   
   
   /*
    * Send 10000 non-persistent messages of 1024 bytes each to single queue transactionally.
    * Transaction size is 100
    * The queue has no consumers.
    * Measure time taken
    */
   public void testQueue3()
   {
      log.info("Running test Queue3");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      SenderJob sender = createDefaultSenderJob(testQueueName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(true);
      sender.setTransactionSize(100);
      sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      long result = runJob(sender).getTestTime();
      
      storeResult("Queue3", result);
      
      log.info("Test Queue3 finished");
   }
   
   
   /*
    * Send 10000 persistent messages of 1024 bytes each to single queue transactionally.
    * Transaction size is 100
    * The queue has no consumers.
    * Measure time taken
    */
   public void testQueue4()
   {
      log.info("Running test Queue4");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      SenderJob sender = createDefaultSenderJob(testQueueName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(true);
      sender.setTransactionSize(100);
      sender.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      long result = runJob(sender).getTestTime();
      
      storeResult("Queue4", result);
      
      log.info("Test Queue4 finished");
   }
   
   /*
    * Send 10000 non-persistent messages of 1024 bytes each to single queue non-transactionally.
    * Concurrently receive them non-transactionally with ackmode=AUTO_ACKNOWLEDGE.
    * Measure total time taken from first send to last receive
    */
   public void testQueue5()
   {
      log.info("Running test Queue5");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      SenderJob sender = createDefaultSenderJob(testQueueName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(false);
      sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testQueueName);
      
      receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
      receiver.setTransacted(false);
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentiall different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Queue5", totalTimeTaken);
      
      log.info("Test Queue5 finished");
   }
   
   
   /*
    * Send 10000 persistent messages of 1024 bytes each to single queue non-transactionally.
    * Concurrently receive them non-transactionally with ackmode=AUTO_ACKNOWLEDGE.
    * Measure total time taken from first send to last receive
    */
   public void testQueue6()
   {
      log.info("Running test Queue6");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      SenderJob sender = createDefaultSenderJob(testQueueName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(false);
      sender.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testQueueName);
      
      receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
      receiver.setTransacted(false);
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Queue6", totalTimeTaken);
      
      log.info("Test Queue6 finished");
   }
   
   /*
    * Send 10000 non-persistent messages of 1024 bytes each to single queue transactionally, transaction size =100
    * Concurrently receive them transactionally with transaction size = 100
    * Measure total time taken from first send to last receive
    */
   public void testQueue7()
   {
      log.info("Running test Queue7");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      SenderJob sender = createDefaultSenderJob(testQueueName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(true);
      sender.setTransactionSize(100);
      sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testQueueName);
      
      receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
      receiver.setTransacted(true);
      receiver.setTransactionSize(100);
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Queue7", totalTimeTaken);
      
      log.info("Test Queue7 finished");
   }
   
   /*
    * Send 10000 persistent messages of 1024 bytes each to single queue transactionally, transaction size =100
    * Concurrently receive them transactionally with transaction size = 100
    * Measure total time taken from first send to last receive
    */
   public void testQueue8()
   {
      log.info("Running test Queue8");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      SenderJob sender = createDefaultSenderJob(testQueueName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(true);
      sender.setTransactionSize(100);
      sender.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testQueueName);
      
      receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
      receiver.setTransacted(true);
      receiver.setTransactionSize(100);
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Queue8", totalTimeTaken);
      
      log.info("Test Queue8 finished");
   }
   
   /*
    * Send 10000 non-persistent messages of 1024 bytes each to single queue non-transactionally
    * Concurrently receive them non-transactionally with ack mode of DUPS_OK_ACKNOWLEDGE
    * Measure total time taken from first send to last receive
    */
   public void testQueue9()
   {
      log.info("Running test Queue9");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      SenderJob sender = createDefaultSenderJob(testQueueName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(false);
      sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testQueueName);
      
      receiver.setAckMode(Session.DUPS_OK_ACKNOWLEDGE);
      receiver.setTransacted(false);
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Queue9", totalTimeTaken);
      
      log.info("Test Queue9 finished");
   }
   
   /*
    * Send 10000 persistent messages of 1024 bytes each to single queue non-transactionally
    * Concurrently receive them non-transactionally with ack mode of DUPS_OK_ACKNOWLEDGE
    * Measure total time taken from first send to last receive
    */
   public void testQueue10()
   {
      log.info("Running test Queue10");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      SenderJob sender = createDefaultSenderJob(testQueueName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(false);
      sender.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testQueueName);
      
      receiver.setAckMode(Session.DUPS_OK_ACKNOWLEDGE);
      receiver.setTransacted(false);
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Queue10", totalTimeTaken);
      
      log.info("Test Queue10 finished");
   }
   
   
   /*
    * Receive only 10000 pre-existing persistent messages of size 1024 bytes from queue.
    * Receive non-transactionally, using ack mode of AUTO_ACKNOWLEDGE
    */
   public void testQueue11()
   {
      log.info("Running test Queue11");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      //Now fill it with 10000 messages
      FillJob fillJob = createDefaultFillJob(testQueueName, 10000);
      fillJob.setDeliveryMode(DeliveryMode.PERSISTENT);
      fillJob.setMsgSize(1024);
      fillJob.setMf(new BytesMessageMessageFactory());
      runJob(fillJob);
      
      ReceiverJob receiver = createDefaultReceiverJob(testQueueName);
      
      receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
      receiver.setTransacted(false);
      
      JobTimings result = runJob(receiver);
      
      storeResult("Queue11", result.getTestTime());
      
      log.info("Test Queue11 finished");
   }
   
   /*
    * Receive only 10000 pre-existing non-persistent messages of size 1024 bytes from queue.
    * Receive non-transactionally, using ack mode of AUTO_ACKNOWLEDGE
    */
   public void testQueue12()
   {
      log.info("Running test Queue12");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      //Now fill it with 10000 messages
      FillJob fillJob = createDefaultFillJob(testQueueName, 10000);
      fillJob.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      fillJob.setMsgSize(1024);
      fillJob.setMf(new BytesMessageMessageFactory());
      runJob(fillJob);
      
      ReceiverJob receiver = createDefaultReceiverJob(testQueueName);
      
      receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
      receiver.setTransacted(false);
      
      JobTimings result = runJob(receiver);
      
      storeResult("Queue12", result.getTestTime());
      
      log.info("Test Queue12 finished");
   }
   
   /*
    * Receive only 10000 pre-existing persistent messages of size 1024 bytes from queue.
    * Receive transactionally with transaction size of 100
    */
   public void testQueue13()
   {
      log.info("Running test Queue13");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      //Now fill it with 10000 messages
      FillJob fillJob = createDefaultFillJob(testQueueName, 10000);
      fillJob.setDeliveryMode(DeliveryMode.PERSISTENT);
      fillJob.setMsgSize(1024);
      fillJob.setMf(new BytesMessageMessageFactory());
      runJob(fillJob);
      
      ReceiverJob receiver = createDefaultReceiverJob(testQueueName);
      
      receiver.setTransacted(true);
      receiver.setTransactionSize(100);
      
      JobTimings result = runJob(receiver);
      
      storeResult("Queue13", result.getTestTime());
      
      log.info("Test Queue13 finished");
   }
   
   /*
    * Receive only 10000 pre-existing non-persistent messages of size 1024 bytes from queue.
    * Receive transactionally with transaction size of 100
    */
   public void testQueue14()
   {
      log.info("Running test Queue14");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      //Now fill it with 10000 messages
      FillJob fillJob = createDefaultFillJob(testQueueName, 10000);
      fillJob.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      fillJob.setMsgSize(1024);
      fillJob.setMf(new BytesMessageMessageFactory());
      runJob(fillJob);
      
      ReceiverJob receiver = createDefaultReceiverJob(testQueueName);
      
      receiver.setTransacted(true);
      receiver.setTransactionSize(100);
      
      JobTimings result = runJob(receiver);
      
      storeResult("Queue14", result.getTestTime());
      
      log.info("Test Queue14 finished");
   }
   
   
   /*
    * Receive only 10000 pre-existing persistent messages of size 1024 bytes from queue.
    * Receive non-transactionally with ack mode of AUTO_ACKNOWLEDGE and selector
    */
   public void testQueue15()
   {
      log.info("Running test Queue15");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      //Now fill it with 10000 messages
      FillJob fillJob = createDefaultFillJob(testQueueName, 10000);
      fillJob.setDeliveryMode(DeliveryMode.PERSISTENT);
      fillJob.setMsgSize(1024);
      fillJob.setMf(new BytesMessageMessageFactory());
      runJob(fillJob);
      
      ReceiverJob receiver = createDefaultReceiverJob(testQueueName);
      
      //This should always be true
      String selector = "JMSMessageID IS NOT NULL AND NonExistentProperty IS NULL";

      receiver.setTransacted(false);
      receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
      receiver.setSelector(selector);
      
      JobTimings result = runJob(receiver);
      
      storeResult("Queue15", result.getTestTime());
      
      log.info("Test Queue15 finished");
   }
   
   /*
    * Receive only 10000 pre-existing non-persistent messages of size 1024 bytes from queue.
    * Receive non-transactionally with ack mode of AUTO_ACKNOWLEDGE and selector
    */
   public void testQueue16()
   {
      log.info("Running test Queue16");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      //Now fill it with 10000 messages
      FillJob fillJob = createDefaultFillJob(testQueueName, 10000);
      fillJob.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      fillJob.setMsgSize(1024);
      fillJob.setMf(new BytesMessageMessageFactory());
      runJob(fillJob);
      
      ReceiverJob receiver = createDefaultReceiverJob(testQueueName);
      
      //This should always be true
      String selector = "JMSMessageID IS NOT NULL AND NonExistentProperty IS NULL";

      
      receiver.setTransacted(false);
      receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
      receiver.setSelector(selector);
      
      JobTimings result = runJob(receiver);
      
      storeResult("Queue16", result.getTestTime());
      
      log.info("Test Queue16 finished");
   }
   
   /*
    * Browse 10000 persistent messages of size 1024 bytes in queue.
    */
   public void testQueue17()
   {
      log.info("Running test Queue17");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      //Now fill it with 10000 messages
      FillJob fillJob = createDefaultFillJob(testQueueName, 10000);
      fillJob.setDeliveryMode(DeliveryMode.PERSISTENT);
      fillJob.setMsgSize(1024);
      fillJob.setMf(new BytesMessageMessageFactory());
      runJob(fillJob);
      
      //TODO
      
      storeResult("Queue17", 0);
      
      log.info("Test Queue17 finished");
   }
   
   /*
    * Browse 10000 non-persistent messages of size 1024 bytes in queue.
    */
   public void testQueue18()
   {
      log.info("Running test Queue18");
      
      //First drain the destination
      runJob(createDefaultDrainJob(testQueueName));
      
      //Now fill it with 10000 messages
      FillJob fillJob = createDefaultFillJob(testQueueName, 10000);
      fillJob.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      fillJob.setMsgSize(1024);
      fillJob.setMf(new BytesMessageMessageFactory());
      runJob(fillJob);
      
      //TODO
      
      storeResult("Queue18", 0);
      
      log.info("Test Queue18 finished");
   }
   
   public void testQueue19()
   {      
      //TODO No-local perf test
   }
   
   /*
    * Send 10000 non-persistent messages of 1024 bytes each to single topic non-transactionally
    * Receive them non-transactionally with single non durable subscriber and ack mode of AUTO_ACKNOWLEDGE
    * Measure total time taken from first send to last receive
    */
   public void testTopic1()
   {
      log.info("Running test Topic1");
      
      SenderJob sender = createDefaultSenderJob(testTopicName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(false);
      sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testTopicName);
      
      receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
      receiver.setTransacted(false);
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Topic1", totalTimeTaken);
      
      log.info("Test Topic1 finished");
   }
   
   /*
    * Send 10000 persistent messages of 1024 bytes each to single topic non-transactionally
    * Receive them non-transactionally with single non durable subscriber and with ack mode of AUTO_ACKNOWLEDGE
    * Measure total time taken from first send to last receive
    */
   public void testTopic2()
   {
      log.info("Running test Topic2");
      
      SenderJob sender = createDefaultSenderJob(testTopicName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(false);
      sender.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testTopicName);
      
      receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
      receiver.setTransacted(false);
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Topic2", totalTimeTaken);
      
      log.info("Test Topic2 finished");
   }
   
   
   /*
    * Send 10000 non-persistent messages of 1024 bytes each to single topic transactionally, tx size = 100
    * Receive them transactionally with single non durable subscriber and with tx size of 100
    * Measure total time taken from first send to last receive
    */
   public void testTopic3()
   {
      log.info("Running test Topic3");
      
      SenderJob sender = createDefaultSenderJob(testTopicName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(true);
      sender.setTransactionSize(100);
      sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testTopicName);
      
      receiver.setTransacted(true);
      receiver.setTransactionSize(100);
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Topic3", totalTimeTaken);
      
      log.info("Test Topic3 finished");
   }
   
   /*
    * Send 10000 persistent messages of 1024 bytes each to single topic transactionally, tx size = 100
    * Receive them transactionally with single non durable subscriber and with tx size of 100
    * Measure total time taken from first send to last receive
    */
   public void testTopic4()
   {
      log.info("Running test Topic4");
      
      SenderJob sender = createDefaultSenderJob(testTopicName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(true);
      sender.setTransactionSize(100);
      sender.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testTopicName);
      
      receiver.setTransacted(true);
      receiver.setTransactionSize(100);
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Topic4", totalTimeTaken);
      
      log.info("Test Topic4 finished");
   }
   
   /*
    * Send 10000 non-persistent messages of 1024 bytes each to single topic non-transactionally
    * Receive them non-transactionally with single non durable subscriber and ack mode of DUPS_OK_ACKNOWLEDGE
    * Measure total time taken from first send to last receive
    */
   public void testTopic5()
   {
      log.info("Running test Topic5");
      
      SenderJob sender = createDefaultSenderJob(testTopicName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(false);
      sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testTopicName);
      
      receiver.setAckMode(Session.DUPS_OK_ACKNOWLEDGE);
      receiver.setTransacted(false);
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Topic5", totalTimeTaken);
      
      log.info("Test Topic5 finished");
   }
   
   /*
    * Send 10000 persistent messages of 1024 bytes each to single topic non-transactionally
    * Receive them non-transactionally with single non durable subscriber and with ack mode of DUPS_OK_ACKNOWLEDGE
    * Measure total time taken from first send to last receive
    */
   public void testTopic6()
   {
      log.info("Running test Topic6");
      
      SenderJob sender = createDefaultSenderJob(testTopicName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(false);
      sender.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testTopicName);
      
      receiver.setAckMode(Session.DUPS_OK_ACKNOWLEDGE);
      receiver.setTransacted(false);
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Topic6", totalTimeTaken);
      
      log.info("Test Topic6 finished");
   }
   
   
   /*
    * Send 10000 non-persistent messages of 1024 bytes each to single topic non-transactionally
    * Receive them non-transactionally with single non durable subscriber and ack mode of AUTO_ACKNOWLEDGE
    * and using a selector
    * Measure total time taken from first send to last receive
    */
   public void testTopic7()
   {
      log.info("Running test Topic7");
      
      SenderJob sender = createDefaultSenderJob(testTopicName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(false);
      sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testTopicName);
      
      receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
      receiver.setTransacted(false);
      
      //This should always be true
      String selector = "JMSMessageID IS NOT NULL AND NonExistentProperty IS NULL";

      receiver.setSelector(selector);
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Topic7", totalTimeTaken);
      
      log.info("Test Topic7 finished");
   }
   
   /*
    * Send 10000 persistent messages of 1024 bytes each to single topic non-transactionally
    * Receive them non-transactionally with single non durable subscriber and with ack mode of AUTO_ACKNOWLEDGE
    * and using selector
    * Measure total time taken from first send to last receive
    */
   public void testTopic8()
   {
      log.info("Running test Topic8");
      
      SenderJob sender = createDefaultSenderJob(testTopicName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(false);
      sender.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testTopicName);
      
      receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
      receiver.setTransacted(false);
      
      //TODO - Choose better selector
      
      //This should always be true
      String selector = "JMSMessageID IS NOT NULL AND NonExistentProperty IS NULL";
      
      receiver.setSelector(selector);
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Topic8", totalTimeTaken);
      
      log.info("Test Topic8 finished");
   }
   
   /*
    * Send 10000 persistent messages of 1024 bytes each to single topic non-transactionally
    * Receive them non-transactionally with single durable subscriber and with ack mode of AUTO_ACKNOWLEDGE
    * Measure total time taken from first send to last receive
    */
   public void testTopic9()
   {
      log.info("Running test Topic9");
      
      //First drain the durable subscription
      DrainJob drainJob = createDefaultDrainJob(testTopicName);
      drainJob.setSubName(testSubscriptionName);
      runJob(drainJob);
      
      SenderJob sender = createDefaultSenderJob(testTopicName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(false);
      sender.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      ReceiverJob receiver = createDefaultReceiverJob(testTopicName);
      
      receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
      receiver.setTransacted(false);
      receiver.setSubName("testSubscription");
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Topic9", totalTimeTaken);
      
      log.info("Test Topic9 finished");
   }
   
   /*
    * Send 10000 persistent messages of 1024 bytes each to single topic transactionally, tx size of 100
    * Receive them transactionally with single durable subscriber, tx size of 100
    * Measure total time taken from first send to last receive
    */
   public void testTopic10()
   {
      log.info("Running test Topic10");
      
      //First drain the durable subscription
      DrainJob drainJob = createDefaultDrainJob(testTopicName);
      drainJob.setSubName(testSubscriptionName);
      runJob(drainJob);
      
      SenderJob sender = createDefaultSenderJob(testTopicName);
      sender.setNumMessages(10000);
      sender.setMsgSize(1024);
      sender.setMf(new BytesMessageMessageFactory());
      sender.setTransacted(true);
      sender.setTransactionSize(100);
      
      ReceiverJob receiver = createDefaultReceiverJob(testTopicName);
      
      receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
      receiver.setTransacted(true);
      receiver.setTransactionSize(100);
      receiver.setSubName("testSubscription");
      
      JobTimings[] results = runJobs(new Job[] {sender, receiver});
      
      /*
       * NB.
       * When we calculating the total time taken from first send to last receive, there is some
       * error involved due to the differences in network latency and other remoting overhead
       * when sending the jobs to be executed on potentially different remote machines.
       * We make the assumption that this this difference is very small compared with time taken
       * to run the tests as to be negligible.
       */
      
      long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
      
      
      storeResult("Topic10", totalTimeTaken);
      
      log.info("Test Topic10 finished");
   }
   
   
   /* Send 1000 persistent messages non-transactionally to topic with one non durable subscriber.
    * Concurrently receive them non-transactionally with ack mode of AUTO_ACKNOWLEDGE
    * Measure total time taken from first send to last receive.
    * Vary the message size and repeat with the following values:
    * 0 bytes
    * 1024 bytes
    * 16384 bytes
    * 65536 bytes
    * 262144 bytes
    * 1048576 bytes
    * 8388608 bytes
    * 
    */
   public void testMessageSizeThroughput()
   {
      log.info("Running test testMessageSizeThroughput");
      
      int[] msgsSize = new int[] {0, 1024, 16384, 65536, 262144, 1048576, 8388608};
      
      for (int i = 0; i < msgsSize.length; i++)
      {
        
         int msgSize = msgsSize[i];
      
         SenderJob sender = createDefaultSenderJob(testTopicName);
         sender.setNumMessages(10000);
         sender.setMsgSize(1024);
         sender.setMf(new BytesMessageMessageFactory());
         sender.setTransacted(false);
         sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         
         ReceiverJob receiver = createDefaultReceiverJob(testTopicName);
         
         receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
         receiver.setTransacted(false);
         
         JobTimings[] results = runJobs(new Job[] {sender, receiver});
         
         /*
          * NB.
          * When we calculating the total time taken from first send to last receive, there is some
          * error involved due to the differences in network latency and other remoting overhead
          * when sending the jobs to be executed on potentially different remote machines.
          * We make the assumption that this this difference is very small compared with time taken
          * to run the tests as to be negligible.
          */
         
         long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
         
         storeResult("testMessageSizeThroughput_" + msgSize, totalTimeTaken);
      }
      
      
      log.info("Test testMessageSizeThroughput finished");
   }
   
   /* Send 10000 non-persistent messages of size 1024 bytes, non transactionally to queue
    * Concurrent receive messages from queue non-transactionally with ack mode AUTO_ACKNOWLEDGE.
    * Measure the throughput as the number of distinct queues is increased from 1 to 10
    */
   public void testQueueScale1()
   {
      log.info("Running test testQueueScale1");
      
      String testQueueNamePrefix = "queue/testQueue";
      
      for (int i = 1; i <= 10; i++)
      {
         //First drain the destinations
         for (int j = 0; j < i; j++)
         {
            runJob(createDefaultDrainJob(testQueueNamePrefix + j));
         }
         
         //Create the jobs
         
         Job[] jobs = new Job[2 * i];
         
         for (int j = 0; j < i; j++)
         {
         
            SenderJob sender = createDefaultSenderJob(testQueueNamePrefix + j);
            jobs[j * 2] = sender;
            sender.setNumMessages(10000);
            sender.setMsgSize(1024);
            sender.setMf(new BytesMessageMessageFactory());
            sender.setTransacted(false);
            sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         
            ReceiverJob receiver = createDefaultReceiverJob(testQueueNamePrefix + j);
            jobs[j * 2 + 1] = receiver;
            
            receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
            receiver.setTransacted(false);
         }
         
         JobTimings[] results = runJobs(jobs);
         
         /*
          * NB.
          * When we calculating the total time taken from first send to last receive, there is some
          * error involved due to the differences in network latency and other remoting overhead
          * when sending the jobs to be executed on potentially different remote machines.
          * We make the assumption that this this difference is very small compared with time taken
          * to run the tests as to be negligible.
          */
         
         long minTimeOfFirstSend = Long.MAX_VALUE;
         long maxTimeOfLastReceive = Long.MIN_VALUE;
         for (int j = 0; j < i; j++)
         {
            JobTimings senderResult = results[j * 2];
            JobTimings receiverResult = results[j * 2 + 1];
            long timeOfFirstSend = senderResult.getInitTime();
            minTimeOfFirstSend = Math.min(minTimeOfFirstSend, timeOfFirstSend);
            long timeOfLastReceive = receiverResult.getInitTime() + receiverResult.getTestTime();
            maxTimeOfLastReceive = Math.max(maxTimeOfLastReceive, timeOfLastReceive);
         }
         
         long totalTimeTaken = maxTimeOfLastReceive - minTimeOfFirstSend;
         long totalMessagesSent = i * 10000;
         
         
         storeResult("QueueScale1", totalTimeTaken);
      }
      
      log.info("Test testQueueScale1 finished");
      
   }
   
   /* Send 10000 non-persistent messages of size 1024 bytes, non transactionally to queue
    * Concurrent receive messages from queue non-transactionally with ack mode AUTO_ACKNOWLEDGE.
    * Measure the throughput as the number of sending connections is increased.
    * Each sending connection has just one session and message producer
    */
   public void testQueueScale2()
   {
      log.info("Running test testQueueScale2");
      
      //TODO - do this across multiple remote clients
      
      for (int i = 1; i <= 20; i++)
      {
         //First drain the destination
         runJob(createDefaultDrainJob(testQueueName));
         
         SenderJob sender = createDefaultSenderJob(testQueueName);
         sender.setNumMessages(10000);
         sender.setMsgSize(1024);
         sender.setMf(new BytesMessageMessageFactory());
         sender.setTransacted(false);
         sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         sender.setNumConnections(i);
         sender.setNumSessions(i);         
      
         ReceiverJob receiver = createDefaultReceiverJob(testQueueName);

         receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
         receiver.setTransacted(false);
         
         
         JobTimings[] results = runJobs(new Job[] {sender, receiver});
         
         /*
          * NB.
          * When we calculating the total time taken from first send to last receive, there is some
          * error involved due to the differences in network latency and other remoting overhead
          * when sending the jobs to be executed on potentially different remote machines.
          * We make the assumption that this this difference is very small compared with time taken
          * to run the tests as to be negligible.
          */
         
         long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
         
         
         storeResult("testQueueScale2_" + i, totalTimeTaken);
      }
      
      log.info("Test testQueueScale2 finished");
      
   }
   
   /* Send 10000 non-persistent messages of size 1024 bytes, non transactionally to queue
    * Concurrent receive messages from queue non-transactionally with ack mode AUTO_ACKNOWLEDGE.
    * Measure the throughput as the number of sending sessions is increased.
    * Each sending session shares the same connection
    */
   public void testQueueScale3()
   {
      log.info("Running test testQueueScale3");
      
      for (int i = 1; i <= 20; i++)
      {
         //First drain the destination
         runJob(createDefaultDrainJob(testQueueName));
         
         SenderJob sender = createDefaultSenderJob(testQueueName);
         sender.setNumMessages(10000);
         sender.setMsgSize(1024);
         sender.setMf(new BytesMessageMessageFactory());
         sender.setTransacted(false);
         sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         sender.setNumConnections(1);
         sender.setNumSessions(i);         
      
         ReceiverJob receiver = createDefaultReceiverJob(testQueueName);

         receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
         receiver.setTransacted(false);
         
         
         JobTimings[] results = runJobs(new Job[] {sender, receiver});
         
         /*
          * NB.
          * When we calculating the total time taken from first send to last receive, there is some
          * error involved due to the differences in network latency and other remoting overhead
          * when sending the jobs to be executed on potentially different remote machines.
          * We make the assumption that this this difference is very small compared with time taken
          * to run the tests as to be negligible.
          */
         
         long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
         
         
         storeResult("testQueueScale3_" + i, totalTimeTaken);
      }
      
      log.info("Test testQueueScale3 finished");
      
   }
   
   /* Send 10000 non-persistent messages of size 1024 bytes, non transactionally to topic
    * Concurrent receive messages from topic non-transactionally with ack mode AUTO_ACKNOWLEDGE.
    * Measure the throughput as the number of distinct topics is increased from 1 to 10
    */
   public void testTopicScale1()
   {
      log.info("Running test testTopicScale1");
      
      String testTopicNamePrefix = "topic/testTopic";
      
      for (int i = 1; i <= 10; i++)
      {                 
         //Create the jobs
         
         Job[] jobs = new Job[2 * i];
         
         for (int j = 0; j < i; j++)
         {
         
            SenderJob sender = createDefaultSenderJob(testTopicNamePrefix + j);
            jobs[j * 2] = sender;
            sender.setNumMessages(10000);
            sender.setMsgSize(1024);
            sender.setMf(new BytesMessageMessageFactory());
            sender.setTransacted(false);
            sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         
            ReceiverJob receiver = createDefaultReceiverJob(testTopicNamePrefix + j);
            jobs[j * 2 + 1] = receiver;
            
            receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
            receiver.setTransacted(false);
         }
         
         JobTimings[] results = runJobs(jobs);
         
         /*
          * NB.
          * When we calculating the total time taken from first send to last receive, there is some
          * error involved due to the differences in network latency and other remoting overhead
          * when sending the jobs to be executed on potentially different remote machines.
          * We make the assumption that this this difference is very small compared with time taken
          * to run the tests as to be negligible.
          */
         
         long minTimeOfFirstSend = Long.MAX_VALUE;
         long maxTimeOfLastReceive = Long.MIN_VALUE;
         for (int j = 0; j < i; j++)
         {
            JobTimings senderResult = results[j * 2];
            JobTimings receiverResult = results[j * 2 + 1];
            long timeOfFirstSend = senderResult.getInitTime();
            minTimeOfFirstSend = Math.min(minTimeOfFirstSend, timeOfFirstSend);
            long timeOfLastReceive = receiverResult.getInitTime() + receiverResult.getTestTime();
            maxTimeOfLastReceive = Math.max(maxTimeOfLastReceive, timeOfLastReceive);
         }
         
         long totalTimeTaken = maxTimeOfLastReceive - minTimeOfFirstSend;
         long totalMessagesSent = i * 10000;
         
         
         storeResult("TopicScale1", totalTimeTaken);
      }
      
      log.info("Test testTopicScale1 finished");
      
   }
   
   /* Send 10000 non-persistent messages of size 1024 bytes, non transactionally to topic
    * Concurrent receive messages from topic non-transactionally with ack mode AUTO_ACKNOWLEDGE.
    * Measure the throughput as the number of receiving topic subscribers is increased.
    * Each topic subscriber uses it's own session and connection
    */
   public void testTopicScale2()
   {
      log.info("Running test testTopicScale2");
      
      //TODO - Spread receivers across remote machines
      
      for (int i = 1; i <= 20; i++)
      {
         SenderJob sender = createDefaultSenderJob(testTopicName);
         sender.setNumMessages(10000);
         sender.setMsgSize(1024);
         sender.setMf(new BytesMessageMessageFactory());
         sender.setTransacted(false);
         sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);                 
      
         ReceiverJob receiver = createDefaultReceiverJob(testQueueName);

         receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
         receiver.setTransacted(false);
         receiver.setNumConnections(i);
         receiver.setNumSessions(i);
         
         
         JobTimings[] results = runJobs(new Job[] {sender, receiver});
         
         /*
          * NB.
          * When we calculating the total time taken from first send to last receive, there is some
          * error involved due to the differences in network latency and other remoting overhead
          * when sending the jobs to be executed on potentially different remote machines.
          * We make the assumption that this this difference is very small compared with time taken
          * to run the tests as to be negligible.
          */
         
         long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
         
         
         storeResult("testTopicScale2_" + i, totalTimeTaken);
      }
      
      log.info("Test testTopicScale2 finished");
      
   }
   
   
   
   /* Send 10000 non-persistent messages of size 1024 bytes, non transactionally to topic
    * Concurrent receive messages from topic non-transactionally with ack mode AUTO_ACKNOWLEDGE.
    * Measure the throughput as the number of receiving topic subscribers is increased.
    * Each topic subscriber uses it's own session but shares a connection
    */
   public void testTopicScale3()
   {
      log.info("Running test testTopicScale3");
      
      for (int i = 1; i <= 20; i++)
      {
         SenderJob sender = createDefaultSenderJob(testTopicName);
         sender.setNumMessages(10000);
         sender.setMsgSize(1024);
         sender.setMf(new BytesMessageMessageFactory());
         sender.setTransacted(false);
         sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);                 
      
         ReceiverJob receiver = createDefaultReceiverJob(testQueueName);

         receiver.setAckMode(Session.AUTO_ACKNOWLEDGE);
         receiver.setTransacted(false);
         receiver.setNumConnections(1);
         receiver.setNumSessions(i);
         
         
         JobTimings[] results = runJobs(new Job[] {sender, receiver});
         
         /*
          * NB.
          * When we calculating the total time taken from first send to last receive, there is some
          * error involved due to the differences in network latency and other remoting overhead
          * when sending the jobs to be executed on potentially different remote machines.
          * We make the assumption that this this difference is very small compared with time taken
          * to run the tests as to be negligible.
          */
         
         long totalTimeTaken = results[1].getTestTime() + results[1].getInitTime() - results[0].getInitTime();
         
         
         storeResult("testTopicScale3_" + i, totalTimeTaken);
      }
      
      log.info("Test testTopicScale3 finished");
      
   }
   
   protected void storeResult(String benchmarkName, long result)
   {
      storeResult(benchmarkName, new Long(result));
   }
   
   protected void storeResult(String benchmarkName, Object result)
   {
      
   }
   
   protected void storeResult(String benchmarkName, Object result, Object argument)
   {
      
   }
   
   protected JobTimings runJob(Job job)
   {
      return sendRequestToSlave(defaultSlaveURL, new RunRequest(job));
   }
   
   /*
    * Run the jobs concurrently
    */
   protected JobTimings[] runJobs(Job[] jobs)
   {      
      JobRunner[] runners = new JobRunner[jobs.length];
      for (int i = 0; i < jobs.length; i++)
      {
         runners[i] = new JobRunner(jobs[i]);
         Thread t = new Thread(runners[i]);
         runners[i].thread = t;
         t.start();
      }
      
      for (int i = 0; i < jobs.length; i++)
      {
         try
         {
            runners[i].thread.join();
         }
         catch (InterruptedException e)
         {}
      } 
      JobTimings[] results = new JobTimings[jobs.length];
      for (int i = 0; i < jobs.length; i++)
      {
         results[i] = runners[i].result;
      }
      return results;
      
   }
   
   class JobRunner implements Runnable
   {
      Job job;
      
      JobTimings result;
      
      Thread thread;
      
      JobRunner(Job job)
      {
         this.job = job;
      }
      
      public void run()
      {
         result = runJob(job);
      }
   }
   
   protected SenderJob createDefaultSenderJob(String destName)
   {
      return new SenderJob(defaultJMSServerURL, destName, 1,
            1, false, 0, 10000,
            false, 1024,
            new BytesMessageMessageFactory(), DeliveryMode.NON_PERSISTENT);           
   }
   
   protected ReceiverJob createDefaultReceiverJob(String destName)
   {
      return new ReceiverJob(defaultJMSServerURL, destName, 1,
            1, false, 0, 10000,
            Session.AUTO_ACKNOWLEDGE, null, null, false, false);
   }
   
   protected DrainJob createDefaultDrainJob(String destName)
   {
      return new DrainJob(defaultJMSServerURL, destName, null);
   }
   
   protected FillJob createDefaultFillJob(String destName, int numMessages)
   {
      return new FillJob(defaultJMSServerURL, destName, numMessages, 1024, new BytesMessageMessageFactory(),
            DeliveryMode.NON_PERSISTENT);      
   }
   
   
   protected JobTimings sendRequestToSlave(String slaveURL, ServerRequest request)
   {
      try
      {
         InvokerLocator locator = new InvokerLocator(slaveURL);
         Client client = new Client(locator, "perftest");
         Object res = client.invoke(request);
         return (JobTimings)res;
      }
      catch (Throwable t)
      {
         log.error("Failed to run job", t);
         return null;
      }
   }
}
