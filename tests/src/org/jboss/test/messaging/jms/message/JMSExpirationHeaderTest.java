/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.message;


import javax.jms.Message;
import javax.jms.DeliveryMode;

import org.jboss.jms.message.JBossMessage;
import EDU.oswego.cs.dl.util.concurrent.Latch;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JMSExpirationHeaderTest extends MessageTest
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private volatile boolean testFailed;
   private volatile long effectiveReceiveTime;
   private volatile Message expectedMessage;

   // Constructors --------------------------------------------------

   public JMSExpirationHeaderTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      expectedMessage = null;
      testFailed = false;
      effectiveReceiveTime = 0;
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Tests ---------------------------------------------------------

   
   
   public void testZeroExpiration() throws Exception
   {
      Message m = queueProducerSession.createMessage();
      queueProducer.send(m);
      assertEquals(Long.MAX_VALUE, queueConsumer.receive().getJMSExpiration());
   }



   public void testNoExpirationOnTimeoutReceive() throws Exception
   {
      Message m = queueProducerSession.createMessage();
      queueProducer.send(m, DeliveryMode.NON_PERSISTENT, 4, 5000);
      Message result = queueConsumer.receive(100);
      assertEquals(m.getJMSMessageID(), result.getJMSMessageID());
   }

   public void testExpirationOnTimeoutReceive() throws Exception
   {
      Message m = queueProducerSession.createMessage();
      queueProducer.send(m, DeliveryMode.NON_PERSISTENT, 4, 1000);
      Thread.sleep(2000);
      assertNull(queueConsumer.receive(100));
   }

   public void testExpiredMessageDiscardingOnTimeoutReceive() throws Exception
   {
      Message m = queueProducerSession.createMessage();
      queueProducer.send(m, DeliveryMode.NON_PERSISTENT, 4, 1000);
      Thread.sleep(2000);

      // start the receiver thread
      final Latch latch = new Latch();
      Thread receiverThread = new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               expectedMessage = queueConsumer.receive(100);
            }
            catch(Exception e)
            {
               log.info("receive() exits with an exception", e);
            }
            finally
            {
               latch.release();
            }
         }
      }, "receiver thread");
      receiverThread.start();

      latch.acquire();
      assertNull(expectedMessage);
   }

   public void testReceiveTimeoutPreservation() throws Exception
   {
      final long timeToWaitForReceive = 5000;

      final Latch receiverLatch = new Latch();

      // start the receiver thread
      Thread receiverThread = new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               long t1 = System.currentTimeMillis();
               expectedMessage = queueConsumer.receive(timeToWaitForReceive);
               //Thread.sleep(10);
               effectiveReceiveTime = System.currentTimeMillis() - t1;
            }
            catch(Exception e)
            {
               log.info("receive() exits with an exception", e);
            }
            finally
            {
               receiverLatch.release();
            }
         }
      }, "receiver thread");
      receiverThread.start();

      final Latch senderLatch = new Latch();

      // start the sender thread
      Thread senderThread = new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               // wait for 3 secs
               Thread.sleep(3000);

               // send an expired message
               Message m = queueProducerSession.createMessage();
               queueProducer.send(m, DeliveryMode.NON_PERSISTENT, 4, -1);

               if (!((JBossMessage)m).isExpired())
               {
                  log.error("The message " + m + " should have expired");
                  testFailed = true;
                  return;
               }
            }
            catch(Exception e)
            {
               log.error("This exception will fail the test", e);
               testFailed = true;
            }
            finally
            {
               senderLatch.release();
            }
         }
      }, "sender thread");
      senderThread.start();


      senderLatch.acquire();
      receiverLatch.acquire();

      if (testFailed)
      {
         fail("Test failed by the sender thread. Watch for exception in logs");
      }

      assertEquals(timeToWaitForReceive, effectiveReceiveTime);
      assertNull(expectedMessage);
   }


   public void testNoExpirationOnReceive() throws Exception
   {
      Message m = queueProducerSession.createMessage();
      queueProducer.send(m, DeliveryMode.NON_PERSISTENT, 4, 5000);
      Message result = queueConsumer.receive();
      assertEquals(m.getJMSMessageID(), result.getJMSMessageID());
   }



   public void testExpirationOnReceive() throws Exception
   {
      expectedMessage = new JBossMessage("placeholder-shouldnt-be-overwritten");

      queueProducer.send(queueProducerSession.createMessage(), DeliveryMode.NON_PERSISTENT, 4, 2000);

      // allow the message to expire
      Thread.sleep(3000);

      final Latch latch = new Latch();
      // blocking read for a while to make sure I don't get anything, not even a null
      Thread receiverThread = new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               log.trace("Attempting to receive");
               expectedMessage = queueConsumer.receive();
               log.trace("Receive exited without exception:" + expectedMessage);
            }
            catch(Exception e)
            {
               log.info("receive() exits with an exception", e);
            }
            catch(Throwable t)
            {
               log.info("receive() exits with an throwable", t);
            }
            finally
            {
               latch.release();
            }
         }
      }, "receiver thread");
      receiverThread.start();

      Thread.sleep(3000);
      receiverThread.interrupt();

      // wait for the reading thread to conclude
      latch.acquire();

      log.trace("Expected message:" + expectedMessage);

      assertEquals("placeholder-shouldnt-be-overwritten", expectedMessage.getJMSMessageID());
   }







   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
