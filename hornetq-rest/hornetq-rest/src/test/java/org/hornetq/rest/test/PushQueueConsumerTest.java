package org.hornetq.rest.test;

import org.hornetq.rest.queue.QueueDeployment;
import org.hornetq.rest.queue.push.HornetQPushStrategy;
import org.hornetq.rest.queue.push.xml.PushRegistration;
import org.hornetq.rest.queue.push.xml.XmlLink;
import org.hornetq.rest.util.Constants;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.PUT;
import javax.ws.rs.Path;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class PushQueueConsumerTest extends MessageTestBase
{
   //   @Test
   public void testBridge() throws Exception
   {
      Link consumeNext = null;
      ClientResponse consumeNextResponse = null;
      Link pushSubscription = null;

      try
      {
         String testName = "testBridge";
         System.out.println("\n" + testName);
         deployQueue(testName);
         deployQueue(testName + "forwardQueue");

         ClientRequest request = new ClientRequest(generateURL("/queues/" + testName));
         ClientResponse response = request.head();
         Assert.assertEquals(200, response.getStatus());
         Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
         System.out.println("create: " + sender);
         Link pushSubscriptions = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "push-consumers");
         System.out.println("push subscriptions: " + pushSubscriptions);

         request = new ClientRequest(generateURL("/queues/" + testName + "forwardQueue"));
         response = request.head();
         Assert.assertEquals(200, response.getStatus());
         Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
         response = consumers.request().formParameter("autoAck", "true").post();
         consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");

         PushRegistration reg = new PushRegistration();
         reg.setDurable(false);
         XmlLink target = new XmlLink();
         target.setHref(generateURL("/queues/" + testName + "forwardQueue"));
         target.setRelationship("destination");
         reg.setTarget(target);
         response = pushSubscriptions.request().body("application/xml", reg).post();
         Assert.assertEquals(201, response.getStatus());
         pushSubscription = response.getLocation();

         ClientResponse res = sender.request().body("text/plain", Integer.toString(1)).post();
         Assert.assertEquals(201, res.getStatus());

         consumeNextResponse = consumeNext.request().header("Accept-Wait", "1").post(String.class);
         Assert.assertEquals(200, res.getStatus());
         Assert.assertEquals("1", res.getEntity(String.class));
      }
      finally
      {
         Link session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), consumeNextResponse, "consumer");
         Assert.assertEquals(204, session.request().delete().getStatus());
         Assert.assertEquals(204, pushSubscription.request().delete().getStatus());
      }
   }

   @Test
   public void testClass() throws Exception
   {
      Link consumePushedMessage = null;
      ClientResponse consumePushedMessageResponse = null;
      Link pushSubscription = null;

      try
      {
         // The name of the queue used for the test should match the name of the test
         String queue = "testClass";
         String queueToPushTo = "pushedFrom-" + queue;
         System.out.println("\n" + queue);

         deployQueue(queue);
         deployQueue(queueToPushTo);

         ClientRequest queueRequest = new ClientRequest(generateURL(getUrlPath(queue)));

         ClientResponse queueResponse = queueRequest.head();
         queueResponse.releaseConnection();
         Assert.assertEquals(200, queueResponse.getStatus());
         Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "create");
//         System.out.println("create: " + sender);
         Link pushSubscriptions = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "push-consumers");
//         System.out.println("push subscriptions: " + pushSubscriptions);

         ClientRequest queueToPushToRequest = new ClientRequest(generateURL(getUrlPath(queueToPushTo)));
         ClientResponse queueToPushToResponse = queueToPushToRequest.head();
         queueToPushToResponse.releaseConnection();
         Assert.assertEquals(200, queueToPushToResponse.getStatus());
         Link pullConsumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueToPushToResponse, "pull-consumers");
//         System.out.println("pull-consumers: " + pullConsumers);
//         System.out.println("setting autoAck on " + pullConsumers);
         ClientResponse autoAckResponse = pullConsumers.request().formParameter("autoAck", "true").post();
         autoAckResponse.releaseConnection();
         consumePushedMessage = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), autoAckResponse, "consume-next");
//         System.out.println("consume-next: " + consumePushedMessage);

         PushRegistration reg = new PushRegistration();
         reg.setDurable(false);
         XmlLink target = new XmlLink();
         target.setHref(generateURL(getUrlPath(queueToPushTo)));
         target.setClassName(HornetQPushStrategy.class.getName());
         reg.setTarget(target);
//         System.out.println("push registration: " + reg);
         ClientResponse pushRegistrationResponse = pushSubscriptions.request().body("application/xml", reg).post();
         pushRegistrationResponse.releaseConnection();
         Assert.assertEquals(201, pushRegistrationResponse.getStatus());
         pushSubscription = pushRegistrationResponse.getLocation();
//         System.out.println("push subscription: " + pushSubscription);

//         System.out.println("sending: 1 to " + sender);
         ClientResponse sendMessageResponse = sender.request().body("text/plain", "1").post();
         sendMessageResponse.releaseConnection();
         Assert.assertEquals(201, sendMessageResponse.getStatus());
//         System.out.println("sent: 1 to " + sender);

//         System.out.println("Getting message from " + consumePushedMessage);
         consumePushedMessageResponse = consumePushedMessage.request().header(Constants.WAIT_HEADER, "1").post(String.class);
         consumePushedMessageResponse.releaseConnection();
         Assert.assertEquals(200, consumePushedMessageResponse.getStatus());
         Assert.assertEquals("1", consumePushedMessageResponse.getEntity(String.class));
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if (consumePushedMessageResponse != null)
         {
            Link consumer = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), consumePushedMessageResponse, "consumer");
            ClientResponse response = consumer.request().delete();
            response.releaseConnection();
            Assert.assertEquals(204, response.getStatus());
         }
         if (pushSubscription != null)
         {
            ClientResponse response = pushSubscription.request().delete();
            response.releaseConnection();
            Assert.assertEquals(204, response.getStatus());
         }
      }
   }

   private String getUrlPath(String queueName)
   {
      return Constants.PATH_FOR_QUEUES + "/" + queueName;
   }

   private void deployQueue(String queueName) throws Exception
   {
      QueueDeployment deployment = new QueueDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName(queueName);
      manager.getQueueManager().deploy(deployment);
   }

   //   @Test
   public void testTemplate() throws Exception
   {
      String testName = "testTemplate";
      System.out.println("\n" + testName);
      deployQueue(testName);
      deployQueue(testName + "forwardQueue");

      ClientRequest request = new ClientRequest(generateURL("/queues/" + testName));

      ClientResponse response = request.head();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link pushSubscriptions = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "push-consumers");
      System.out.println("push subscriptions: " + pushSubscriptions);

      request = new ClientRequest(generateURL("/queues/" + testName + "forwardQueue"));
      response = request.head();
      Assert.assertEquals(200, response.getStatus());
      Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      Link createWithId = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create-with-id");
      response = consumers.request().formParameter("autoAck", "true").post();
      Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");

      PushRegistration reg = new PushRegistration();
      reg.setDurable(false);
      XmlLink target = new XmlLink();
      target.setRelationship("template");
      target.setHref(createWithId.getHref());
      reg.setTarget(target);
      response = pushSubscriptions.request().body("application/xml", reg).post();
      Assert.assertEquals(201, response.getStatus());
      Link pushSubscription = response.getLocation();

      ClientResponse res = sender.request().body("text/plain", Integer.toString(1)).post();
      Assert.assertEquals(201, res.getStatus());

      res = consumeNext.request().header("Accept-Wait", "1").post(String.class);
      Assert.assertEquals(200, res.getStatus());
      Assert.assertEquals("1", res.getEntity(String.class));
      Link session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
      Assert.assertEquals(204, session.request().delete().getStatus());
      Assert.assertEquals(204, pushSubscription.request().delete().getStatus());
   }

   @Path("/my")
   public static class MyResource
   {
      public static String gotit;

      @PUT
      public void put(String str)
      {
         gotit = str;
      }

   }

   //   @Test
   public void testUri() throws Exception
   {
      String testName = "testUri";
      System.out.println("\n" + testName);
      deployQueue(testName);
      deployQueue(testName + "forwardQueue");

      ClientRequest request = new ClientRequest(generateURL("/queues/" + testName));
      server.getJaxrsServer().getDeployment().getRegistry().addPerRequestResource(MyResource.class);

      ClientResponse response = request.head();
      Assert.assertEquals(200, response.getStatus());
      Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
      System.out.println("create: " + sender);
      Link pushSubscriptions = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "push-consumers");
      System.out.println("push subscriptions: " + pushSubscriptions);

      PushRegistration reg = new PushRegistration();
      reg.setDurable(false);
      XmlLink target = new XmlLink();
      target.setMethod("put");
      target.setHref(generateURL("/my"));
      reg.setTarget(target);
      response = pushSubscriptions.request().body("application/xml", reg).post();
      Assert.assertEquals(201, response.getStatus());
      Link pushSubscription = response.getLocation();

      ClientResponse res = sender.request().body("text/plain", Integer.toString(1)).post();
      Assert.assertEquals(201, res.getStatus());

      Thread.sleep(100);

      Assert.assertEquals("1", MyResource.gotit);
      Assert.assertEquals(204, pushSubscription.request().delete().getStatus());
   }

}
