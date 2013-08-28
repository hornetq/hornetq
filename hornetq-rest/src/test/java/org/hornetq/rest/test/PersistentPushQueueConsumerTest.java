package org.hornetq.rest.test;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.rest.MessageServiceManager;
import org.hornetq.rest.queue.QueueDeployment;
import org.hornetq.rest.queue.push.xml.PushRegistration;
import org.hornetq.rest.queue.push.xml.XmlLink;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.jboss.resteasy.test.EmbeddedContainer;
import org.junit.Assert;
import org.junit.Test;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

/**
 * Test durable queue push consumers
 *
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class PersistentPushQueueConsumerTest
{
   public static MessageServiceManager manager;
   protected static ResteasyDeployment deployment;
   public static HornetQServer hornetqServer;

   public static void startup() throws Exception
   {
      Configuration configuration = new ConfigurationImpl();
      configuration.setPersistenceEnabled(false);
      configuration.setSecurityEnabled(false);
      configuration.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

      hornetqServer = HornetQServers.newHornetQServer(configuration);
      hornetqServer.start();

      deployment = EmbeddedContainer.start();
      manager = new MessageServiceManager();
      manager.start();
      deployment.getRegistry().addSingletonResource(manager.getQueueManager().getDestination());
      deployment.getRegistry().addSingletonResource(manager.getTopicManager().getDestination());
   }

   public static void shutdown() throws Exception
   {
      manager.stop();
      manager = null;
      EmbeddedContainer.stop();
      deployment = null;
      hornetqServer.stop();
      hornetqServer = null;
   }

   @Test
   public void testBridge() throws Exception
   {
      try
      {
         startup();

         String testName = "testBridge";
         deployBridgeQueues(testName);

         ClientRequest request = new ClientRequest(generateURL("/queues/" + testName));

         ClientResponse<?> response = request.head();
         response.releaseConnection();
         Assert.assertEquals(200, response.getStatus());
         Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
         System.out.println("create: " + sender);
         Link pushSubscriptions = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "push-consumers");
         System.out.println("push subscriptions: " + pushSubscriptions);

         request = new ClientRequest(generateURL("/queues/" + testName + "forwardQueue"));
         response = request.head();
         response.releaseConnection();
         Assert.assertEquals(200, response.getStatus());
         Link consumers = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "pull-consumers");
         System.out.println("pull: " + consumers);
         response = Util.setAutoAck(consumers, true);
         Link consumeNext = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "consume-next");
         System.out.println("poller: " + consumeNext);

         PushRegistration reg = new PushRegistration();
         reg.setDurable(true);
         reg.setDisableOnFailure(true);
         XmlLink target = new XmlLink();
         target.setHref(generateURL("/queues/" + testName + "forwardQueue"));
         target.setRelationship("destination");
         reg.setTarget(target);
         response = pushSubscriptions.request().body("application/xml", reg).post();
         response.releaseConnection();
         Assert.assertEquals(201, response.getStatus());

         shutdown();
         startup();
         deployBridgeQueues(testName);

        ClientResponse<?> res = sender.request().body("text/plain", Integer.toString(1)).post();
         res.releaseConnection();
         Assert.assertEquals(201, res.getStatus());

         res = consumeNext.request().header("Accept-Wait", "2").post(String.class);

         Assert.assertEquals(200, res.getStatus());
         Assert.assertEquals("1", res.getEntity(String.class));
         res.releaseConnection();
         Link session = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), res, "consumer");
         res = session.request().delete();
         res.releaseConnection();
         Assert.assertEquals(204, res.getStatus());

         manager.getQueueManager().getPushStore().removeAll();
      }
      finally
      {
         shutdown();
      }
   }

   private void deployBridgeQueues(String testName) throws Exception
   {
      QueueDeployment deployment = new QueueDeployment();
      deployment.setDuplicatesAllowed(true);
      deployment.setDurableSend(false);
      deployment.setName(testName);
      manager.getQueueManager().deploy(deployment);
      QueueDeployment deployment2 = new QueueDeployment();
      deployment2.setDuplicatesAllowed(true);
      deployment2.setDurableSend(false);
      deployment2.setName(testName + "forwardQueue");
      manager.getQueueManager().deploy(deployment2);
   }

   @Test
   public void testFailure() throws Exception
   {
      try
      {
         startup();

         String testName = "testFailure";
         QueueDeployment deployment = new QueueDeployment();
         deployment.setDuplicatesAllowed(true);
         deployment.setDurableSend(false);
         deployment.setName(testName);
         manager.getQueueManager().deploy(deployment);

         ClientRequest request = new ClientRequest(generateURL("/queues/" + testName));

         ClientResponse<?> response = request.head();
         response.releaseConnection();
         Assert.assertEquals(200, response.getStatus());
         Link sender = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "create");
         System.out.println("create: " + sender);
         Link pushSubscriptions = MessageTestBase.getLinkByTitle(manager.getQueueManager().getLinkStrategy(), response, "push-consumers");
         System.out.println("push subscriptions: " + pushSubscriptions);

         PushRegistration reg = new PushRegistration();
         reg.setDurable(true);
         XmlLink target = new XmlLink();
         target.setHref("http://localhost:3333/error");
         target.setRelationship("uri");
         reg.setTarget(target);
         reg.setDisableOnFailure(true);
         reg.setMaxRetries(3);
         reg.setRetryWaitMillis(10);
         response = pushSubscriptions.request().body("application/xml", reg).post();
         Assert.assertEquals(201, response.getStatus());
         Link pushSubscription = response.getLocation();
         response.releaseConnection();

        ClientResponse<?> res = sender.request().body("text/plain", Integer.toString(1)).post();
         res.releaseConnection();
         Assert.assertEquals(201, res.getStatus());

         Thread.sleep(5000);

         response = pushSubscription.request().get();
         PushRegistration reg2 = response.getEntity(PushRegistration.class);
         Assert.assertEquals(reg.isDurable(), reg2.isDurable());
         Assert.assertEquals(reg.getTarget().getHref(), reg2.getTarget().getHref());
         Assert.assertFalse(reg2.isEnabled()); // make sure the failure disables the PushRegistration
         response.releaseConnection();

         manager.getQueueManager().getPushStore().removeAll();
      }
      finally
      {
         shutdown();
      }
   }
}