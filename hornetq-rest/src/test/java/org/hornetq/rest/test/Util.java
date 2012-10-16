package org.hornetq.rest.test;

import org.hornetq.rest.util.Constants;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;

public final class Util
{
   private Util()
   {
      // Utility class
   }

   static ClientResponse head(ClientRequest request) throws Exception
   {
      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      return response;
   }

   static String getUrlPath(String queueName)
   {
      return Constants.PATH_FOR_QUEUES + "/" + queueName;
   }

   static ClientResponse setAutoAck(Link link, boolean ack) throws Exception
   {
      ClientResponse response;
      response = link.request().formParameter("autoAck", Boolean.toString(ack)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());
      return response;
   }
}
