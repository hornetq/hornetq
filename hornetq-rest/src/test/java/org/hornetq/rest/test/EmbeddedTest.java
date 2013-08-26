package org.hornetq.rest.test;
import org.junit.Before;
import org.junit.After;

import static org.jboss.resteasy.test.TestPortProvider.generateURL;

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.hornetq.rest.HttpHeaderProperty;
import org.hornetq.rest.integration.EmbeddedRestHornetQJMS;
import org.hornetq.spi.core.naming.BindingRegistry;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 */
public class EmbeddedTest
{
   public static EmbeddedRestHornetQJMS server;

   @BeforeClass
   public static void startEmbedded() throws Exception
   {
      server = new EmbeddedRestHornetQJMS();
      server.getManager().setConfigResourcePath("hornetq-rest.xml");
      server.start();
   }

   @AfterClass
   public static void stopEmbedded() throws Exception
   {
      server.stop();
      server = null;
   }

   public static void publish(String destination, Serializable object, String contentType) throws Exception
   {
      BindingRegistry reg = server.getRegistry();
      Destination dest = (Destination) reg.lookup(destination);
      ConnectionFactory factory = (ConnectionFactory) reg.lookup("ConnectionFactory");
      Connection conn = factory.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      try
      {
         Assert.assertNotNull("Destination was null", dest);
         MessageProducer producer = session.createProducer(dest);
         ObjectMessage message = session.createObjectMessage();

         if (contentType != null)
         {
            message.setStringProperty(HttpHeaderProperty.CONTENT_TYPE, contentType);
         }
         message.setObject(object);

         producer.send(message);
      }
      finally
      {
         conn.close();
      }
   }


   @Test
   public void testTransform() throws Exception
   {

      ClientRequest request = new ClientRequest(generateURL("/queues/jms.queue.exampleQueue"));

      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      Link sender = response.getLinkHeader().getLinkByTitle("create");
      System.out.println("create: " + sender);
      Link consumers = response.getLinkHeader().getLinkByTitle("pull-consumers");
      System.out.println("pull: " + consumers);
      response = Util.setAutoAck(consumers, true);
      Link consumeNext = response.getLinkHeader().getLinkByTitle("consume-next");
      System.out.println("consume-next: " + consumeNext);

      // test that Accept header is used to set content-type
      {
         TransformTest.Order order = new TransformTest.Order();
         order.setName("1");
         order.setAmount("$5.00");
         publish("/queue/exampleQueue", order, null);

        ClientResponse<?> res = consumeNext.request().header("Accept-Wait", "2").accept("application/xml").post(String.class);
         Assert.assertEquals(200, res.getStatus());
         Assert.assertEquals("application/xml", res.getHeaders().getFirst("Content-Type").toString().toLowerCase());
         TransformTest.Order order2 = res.getEntity(TransformTest.Order.class);
         Assert.assertEquals(order, order2);
         consumeNext = res.getLinkHeader().getLinkByTitle("consume-next");
         res.releaseConnection();
         Assert.assertNotNull(consumeNext);
      }

      // test that Accept header is used to set content-type
      {
         TransformTest.Order order = new TransformTest.Order();
         order.setName("1");
         order.setAmount("$5.00");
         publish("/queue/exampleQueue", order, null);

        ClientResponse<?> res = consumeNext.request().header("Accept-Wait", "2").accept("application/json").post(String.class);
         Assert.assertEquals(200, res.getStatus());
         Assert.assertEquals("application/json", res.getHeaders().getFirst("Content-Type").toString().toLowerCase());
         TransformTest.Order order2 = res.getEntity(TransformTest.Order.class);
         Assert.assertEquals(order, order2);
         consumeNext = res.getLinkHeader().getLinkByTitle("consume-next");
         res.releaseConnection();
         Assert.assertNotNull(consumeNext);
      }

      // test that message property is used to set content type
      {
         TransformTest.Order order = new TransformTest.Order();
         order.setName("2");
         order.setAmount("$15.00");
         publish("/queue/exampleQueue", order, "application/xml");

        ClientResponse<?> res = consumeNext.request().header("Accept-Wait", "2").post(String.class);
         Assert.assertEquals(200, res.getStatus());
         Assert.assertEquals("application/xml", res.getHeaders().getFirst("Content-Type").toString().toLowerCase());
         TransformTest.Order order2 = res.getEntity(TransformTest.Order.class);
         Assert.assertEquals(order, order2);
         consumeNext = res.getLinkHeader().getLinkByTitle("consume-next");
         res.releaseConnection();
         Assert.assertNotNull(consumeNext);
      }
   }
}
