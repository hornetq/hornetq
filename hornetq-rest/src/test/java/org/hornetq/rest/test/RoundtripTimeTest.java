package org.hornetq.rest.test;

import org.hornetq.rest.queue.QueueDeployment;
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
public class RoundtripTimeTest extends MessageTestBase
{
   @Test
   public void testSuccessFirst() throws Exception
   {
      QueueDeployment deployment = new QueueDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName("testQueue");
      manager.getQueueManager().deploy(deployment);

      ClientRequest request = new ClientRequest(generateURL("/queues/testQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
      System.out.println("consume-next: " + consumeNext);


      long start = System.currentTimeMillis();
      int num = 100;
      for (int i = 0; i < num; i++)
      {
         response = sender.request().body("text/plain", Integer.toString(i + 1)).post();
         response.releaseConnection();
      }
      long end = System.currentTimeMillis() - start;
      System.out.println(num + " iterations took " + end + "ms");

      for (int i = 0; i < num; i++)
      {
         response = consumeNext.request().post(String.class);
         consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
         Assert.assertEquals(200, response.getStatus());
         Assert.assertEquals(Integer.toString(i + 1), response.getEntity(String.class));
         response.releaseConnection();
      }
   }

}