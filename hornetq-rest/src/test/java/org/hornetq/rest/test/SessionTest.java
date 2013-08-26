package org.hornetq.rest.test;
import org.junit.Before;

import org.hornetq.rest.queue.QueueDeployment;
import org.hornetq.rest.topic.TopicDeployment;
import org.hornetq.rest.util.Constants;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class SessionTest extends MessageTestBase
{
   @BeforeClass
   public static void setup() throws Exception
   {
      QueueDeployment deployment1 = new QueueDeployment("testQueue", true);
      manager.getQueueManager().deploy(deployment1);
      TopicDeployment deployment = new TopicDeployment();
      deployment.setConsumerSessionTimeoutSeconds(1);
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testTopic");
      manager.getTopicManager().deploy(deployment);
   }

   @Test
   public void testRestartFromAutoAckSession() throws Exception
   {
      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link session = response.getLocation();
      response = session.request().head();
      response.releaseConnection();
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("consume-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().get();
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().head();
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");

      response = sender.request().body("text/plain", Integer.toString(3)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("3", response.getEntity(String.class));
      response.releaseConnection();

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }


   @Test
   public void testTopicRestartFromAutoAckSession() throws Exception
   {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link session = response.getLocation();
      response = session.request().head();
      response.releaseConnection();
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("consume-next: " + consumeNext);

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().get();
      response.releaseConnection();
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().head();
      response.releaseConnection();
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");

      response = sender.request().body("text/plain", Integer.toString(3)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      response = consumeNext.request().post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("3", response.getEntity(String.class));
      response.releaseConnection();

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }


   @Test
   public void testRestartFromAckSession() throws Exception
   {
      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link session = response.getLocation();
      response = session.request().head();
      response.releaseConnection();
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("consume-next: " + consumeNext);
      Link ack = null;

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      // consume
      response = consumeNext.request().header(Constants.WAIT_HEADER, "3").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().get();
      response.releaseConnection();
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNull(consumeNext);
      ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNotNull(ack);

      // acknowledge
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      response = session.request().head();
      response.releaseConnection();
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNotNull(consumeNext);
      ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNull(ack);

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      // consume
      response = consumeNext.request().header(Constants.WAIT_HEADER, "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().get();
      response.releaseConnection();
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNull(consumeNext);
      ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNotNull(ack);

      // acknowledge
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      response = session.request().head();
      response.releaseConnection();
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNotNull(consumeNext);
      ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNull(ack);

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }

   @Test
   public void testTopicRestartFromAckSession() throws Exception
   {
      ClientRequest request = new ClientRequest(generateURL("/topics/testTopic"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-subscriptions");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, false);
      Link session = response.getLocation();
      response = session.request().head();
      response.releaseConnection();
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      System.out.println("consume-next: " + consumeNext);
      Link ack = null;

      response = sender.request().body("text/plain", Integer.toString(1)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      // consume
      response = consumeNext.request().header(Constants.WAIT_HEADER, "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("1", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().get();
      response.releaseConnection();
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNull(consumeNext);
      ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNotNull(ack);

      // acknowledge
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      response = session.request().head();
      response.releaseConnection();
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNotNull(consumeNext);
      ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNull(ack);

      response = sender.request().body("text/plain", Integer.toString(2)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());

      // consume
      response = consumeNext.request().header(Constants.WAIT_HEADER, "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals("2", response.getEntity(String.class));
      response.releaseConnection();
      response = session.request().get();
      response.releaseConnection();
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNull(consumeNext);
      ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNotNull(ack);

      // acknowledge
      response = ack.request().formParameter("acknowledge", "true").post();
      response.releaseConnection();
      response = session.request().head();
      response.releaseConnection();
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledge-next");
      Assert.assertNotNull(consumeNext);
      ack = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "acknowledgement");
      Assert.assertNull(ack);

      response = session.request().delete();
      response.releaseConnection();
      Assert.assertEquals(204, response.getStatus());
   }
}
