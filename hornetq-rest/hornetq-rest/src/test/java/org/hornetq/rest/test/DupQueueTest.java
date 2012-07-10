package org.hornetq.rest.test;

import org.hornetq.rest.queue.QueueDeployment;
import org.hornetq.rest.util.Constants;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;
import org.junit.Test;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class DupQueueTest extends MessageTestBase
{
   @Test
   public void testDup() throws Exception
   {
      String testName = "testDup";
      QueueDeployment deployment = new QueueDeployment();
      deployment.setDuplicatesAllowed(false);
      deployment.setDurableSend(false);
      deployment.setName(testName);
      manager.getQueueManager().deploy(deployment);

      ClientRequest request = new ClientRequest(generateURL("/queues/" + testName));

      ClientResponse response = request.head();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("poller: " + consumeNext);

      ClientResponse res = sender.request().body("text/plain", Integer.toString(1)).post();
      Assert.assertEquals(307, res.getStatus());
      sender = res.getLocation();
      res.releaseConnection();
      System.out.println("create-next: " + sender);
      Assert.assertNotNull(sender);
      res = sender.request().body("text/plain", Integer.toString(1)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      res = sender.request().body("text/plain", Integer.toString(1)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "create-next");
      res = sender.request().body("text/plain", Integer.toString(2)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      res = consumeNext.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("1", res.getEntity(String.class));
      res.releaseConnection();
      Link session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
      System.out.println("session: " + session);
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consume-next");
      System.out.println("consumeNext: " + consumeNext);

      res = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("2", res.getEntity(String.class));
      res.releaseConnection();
      session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
      System.out.println("session: " + session);
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consume-next");
      System.out.println("consumeNext: " + consumeNext);
      res = consumeNext.request().post(String.class);
      res.releaseConnection();
      Assert.assertEquals(503, res.getStatus());

      res = session.request().delete();
      res.releaseConnection();
      Assert.assertEquals(204, res.getStatus());
   }

   @Test
   public void testDupWithId() throws Exception
   {
      String testName = "testDupWithId";
      QueueDeployment deployment = new QueueDeployment();
      deployment.setDuplicatesAllowed(false);
      deployment.setDurableSend(false);
      deployment.setName(testName);
      manager.getQueueManager().deploy(deployment);

      ClientRequest request = new ClientRequest(generateURL("/queues/" + testName));

      ClientResponse response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create-with-id");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("poller: " + consumeNext);

      ClientResponse res = sender.request().pathParameter("id", "1").body("text/plain", Integer.toString(1)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      res = sender.request().body("text/plain", Integer.toString(1)).pathParameter("id", "1").post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());
      sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "create-next");
      res = sender.request().body("text/plain", Integer.toString(2)).post();
      res.releaseConnection();
      Assert.assertEquals(201, res.getStatus());

      res = consumeNext.request().post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("1", res.getEntity(String.class));
      res.releaseConnection();
      Link session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
      System.out.println("session: " + session);
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consume-next");
      System.out.println("consumeNext: " + consumeNext);

      res = consumeNext.request().header(Constants.WAIT_HEADER, "10").post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("2", res.getEntity(String.class));
      res.releaseConnection();
      session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
      System.out.println("session: " + session);
      consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consume-next");
      System.out.println("consumeNext: " + consumeNext);
      res = consumeNext.request().post(String.class);
      res.releaseConnection();
      Assert.assertEquals(503, res.getStatus());

      res = session.request().delete();
      res.releaseConnection();
      Assert.assertEquals(204, res.getStatus());
   }
}