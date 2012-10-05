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
   enum PushRegistrationType
   {
      CLASS, BRIDGE, URI, TEMPLATE
   }

   @Test
   public void testBridge() throws Exception
   {
      Link destinationForConsumption = null;
      ClientResponse consumerResponse = null;
      Link pushSubscription = null;
      String messageContent = "1";

      try
      {
         // The name of the queue used for the test should match the name of the test
         String queue = "testBridge";
         String queueToPushTo = "pushedFrom-" + queue;
         System.out.println("\n" + queue);
         deployQueue(queue);
         deployQueue(queueToPushTo);

         ClientResponse queueResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queue))));
         Link destination = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "create");
         Link pushSubscriptions = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "push-consumers");

         ClientResponse queueToPushToResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queueToPushTo))));
         ClientResponse autoAckResponse = setAutoAck(queueToPushToResponse, true);
         destinationForConsumption = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), autoAckResponse, "consume-next");

         pushSubscription = createPushRegistration(queueToPushTo, pushSubscriptions, PushRegistrationType.BRIDGE);

         sendMessage(destination, messageContent);

         consumerResponse = consume(destinationForConsumption, messageContent);
      }
      finally
      {
         cleanupConsumer(consumerResponse);
         cleanupSubscription(pushSubscription);
      }
   }

   private void cleanupSubscription(Link pushSubscription) throws Exception
   {
      if (pushSubscription != null)
      {
         ClientResponse<?> response = pushSubscription.request().delete();
         response.releaseConnection();
         Assert.assertEquals(204, response.getStatus());
      }
   }

//   @Test
   public void testClass() throws Exception
   {
      Link destinationForConsumption = null;
      ClientResponse consumerResponse = null;
      Link pushSubscription = null;
      String messageContent = "1";

      try
      {
         // The name of the queue used for the test should match the name of the test
         String queue = "testClass";
         String queueToPushTo = "pushedFrom-" + queue;
         System.out.println("\n" + queue);

         deployQueue(queue);
         deployQueue(queueToPushTo);

         ClientResponse queueResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queue))));
         Link destinationForSend = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "create");
         Link pushSubscriptions = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "push-consumers");

         ClientResponse queueToPushToResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queueToPushTo))));
         ClientResponse autoAckResponse = setAutoAck(queueToPushToResponse, true);
         destinationForConsumption = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), autoAckResponse, "consume-next");

         pushSubscription = createPushRegistration(queueToPushTo, pushSubscriptions, PushRegistrationType.CLASS);

         sendMessage(destinationForSend, messageContent);

         consumerResponse = consume(destinationForConsumption, messageContent);
      }
      finally
      {
         cleanupConsumer(consumerResponse);
         cleanupSubscription(pushSubscription);
      }
   }

//   @Test
   public void testTemplate() throws Exception
   {
      Link destinationForConsumption = null;
      ClientResponse consumerResponse = null;
      Link pushSubscription = null;
      String messageContent = "1";

      try
      {
         // The name of the queue used for the test should match the name of the test
         String queue = "testTemplate";
         String queueToPushTo = "pushedFrom-" + queue;
         System.out.println("\n" + queue);

         deployQueue(queue);
         deployQueue(queueToPushTo);

         ClientResponse queueResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queue))));
         Link destinationForSend = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "create");
         Link pushSubscriptions = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "push-consumers");

         ClientResponse queueToPushToResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queueToPushTo))));
         Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueToPushToResponse, "pull-consumers");
         Link createWithId = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueToPushToResponse, "create-with-id");
         ClientResponse autoAckResponse = Util.setAutoAck(consumers, true);
         destinationForConsumption = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), autoAckResponse, "consume-next");

         pushSubscription = createPushRegistration(createWithId.getHref(), pushSubscriptions, PushRegistrationType.TEMPLATE);

         sendMessage(destinationForSend, messageContent);

         consumerResponse = consume(destinationForConsumption, messageContent);
      }
      finally
      {
         cleanupConsumer(consumerResponse);
         cleanupSubscription(pushSubscription);
      }
   }

   @Path("/my")
   public static class MyResource
   {
      public static String got_it;

      @PUT
      public void put(String str)
      {
         got_it = str;
      }

   }

//   @Test
   public void testUri() throws Exception
   {
      Link pushSubscription = null;
      String messageContent = "1";

      try
      {
         // The name of the queue used for the test should match the name of the test
         String queue = "testUri";
         String queueToPushTo = "pushedFrom-" + queue;
         System.out.println("\n" + queue);

         deployQueue(queue);
         deployQueue(queueToPushTo);
         server.getJaxrsServer().getDeployment().getRegistry().addPerRequestResource(MyResource.class);

         ClientResponse queueResponse = Util.head(new ClientRequest(generateURL(Util.getUrlPath(queue))));
         Link destinationForSend = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "create");
         Link pushSubscriptions = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), queueResponse, "push-consumers");

         pushSubscription = createPushRegistration(generateURL("/my"), pushSubscriptions, PushRegistrationType.URI);

         sendMessage(destinationForSend, messageContent);

         Thread.sleep(100);

         Assert.assertEquals(messageContent, MyResource.got_it);
      }
      finally
      {
         cleanupSubscription(pushSubscription);
      }
   }

   private void deployQueue(String queueName) throws Exception
   {
      QueueDeployment deployment = new QueueDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName(queueName);
      manager.getQueueManager().deploy(deployment);
   }

   private ClientResponse consume(Link destination, String expectedContent) throws Exception
   {
      ClientResponse response;
      response = destination.request().header(Constants.WAIT_HEADER, "1").post(String.class);
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals(expectedContent, response.getEntity(String.class));
      response.releaseConnection();
      return response;
   }

   private void sendMessage(Link sender, String content) throws Exception
   {
      ClientResponse sendMessageResponse = sender.request().body("text/plain", content).post();
      sendMessageResponse.releaseConnection();
      Assert.assertEquals(201, sendMessageResponse.getStatus());
   }

   private ClientResponse setAutoAck(ClientResponse response, boolean ack) throws Exception
   {
      Link pullConsumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
      ClientResponse autoAckResponse = pullConsumers.request().formParameter("autoAck", Boolean.toString(ack)).post();
      autoAckResponse.releaseConnection();
      Assert.assertEquals(201, autoAckResponse.getStatus());
      return autoAckResponse;
   }

   private void cleanupConsumer(ClientResponse consumerResponse) throws Exception
   {
      if (consumerResponse != null)
      {
         Link consumer = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), consumerResponse, "consumer");
         ClientResponse<?> response = consumer.request().delete();
         response.releaseConnection();
         Assert.assertEquals(204, response.getStatus());
      }
   }

   private Link createPushRegistration(String queueToPushTo, Link pushSubscriptions, PushRegistrationType pushRegistrationType) throws Exception
   {
      PushRegistration reg = new PushRegistration();
      reg.setDurable(false);
      XmlLink target = new XmlLink();
      if (pushRegistrationType == PushRegistrationType.CLASS)
      {
         target.setHref(generateURL(Util.getUrlPath(queueToPushTo)));
         target.setClassName(HornetQPushStrategy.class.getName());
      }
      else if (pushRegistrationType == PushRegistrationType.BRIDGE)
      {
         target.setHref(generateURL(Util.getUrlPath(queueToPushTo)));
         target.setRelationship("destination");
      }
      else if (pushRegistrationType == PushRegistrationType.TEMPLATE)
      {
         target.setHref(queueToPushTo);
         target.setRelationship("template");
      }
      else if (pushRegistrationType == PushRegistrationType.URI)
      {
         target.setMethod("put");
         target.setHref(queueToPushTo);
      }
      reg.setTarget(target);
      ClientResponse pushRegistrationResponse = pushSubscriptions.request().body("application/xml", reg).post();
      pushRegistrationResponse.releaseConnection();
      Assert.assertEquals(201, pushRegistrationResponse.getStatus());
      Link pushSubscription = pushRegistrationResponse.getLocation();
      return pushSubscription;
   }
}
