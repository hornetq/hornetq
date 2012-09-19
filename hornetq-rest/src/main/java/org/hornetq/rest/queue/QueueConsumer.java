package org.hornetq.rest.queue;

import java.net.URI;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.jms.client.SelectorTranslator;
import org.hornetq.rest.util.Constants;
import org.hornetq.rest.util.LinkStrategy;

/**
 * Auto-acknowleged consumer
 *
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 */
public class QueueConsumer
{
   protected ClientSessionFactory factory;
   protected ClientSession session;
   protected ClientConsumer consumer;
   protected String destination;
   protected boolean closed;
   protected String id;
   protected long lastPing = System.currentTimeMillis();
   protected DestinationServiceManager serviceManager;
   protected boolean autoAck = true;
   protected String selector;

   /**
    * token used to create consume-next links
    */
   protected long previousIndex = -1;
   protected ConsumedMessage lastConsumed;

   public long getConsumeIndex()
   {
      if (lastConsumed == null) return -1;
      return lastConsumed.getMessageID();
   }

   public DestinationServiceManager getServiceManager()
   {
      return serviceManager;
   }

   public void setServiceManager(DestinationServiceManager serviceManager)
   {
      this.serviceManager = serviceManager;
   }

   public long getLastPingTime()
   {
      return lastPing;
   }

   protected void ping()
   {
      lastPing = System.currentTimeMillis();
   }

   public QueueConsumer(ClientSessionFactory factory, String destination, String id, DestinationServiceManager serviceManager, String selector) throws HornetQException
   {
      this.factory = factory;
      this.destination = destination;
      this.id = id;
      this.serviceManager = serviceManager;
      this.selector = selector;

      createSession();
   }

   public String getId()
   {
      return id;
   }

   public boolean isClosed()
   {
      return closed;
   }

   public synchronized void shutdown()
   {
      if (closed) return;
      closed = true;
      lastConsumed = null;
      previousIndex = -2;
      try
      {
         consumer.close();
      }
      catch (Exception e)
      {
      }

      try
      {
         session.close();
      }
      catch (Exception e)
      {
      }
      session = null;
      consumer = null;
   }


   @Path("consume-next{index}")
   @POST
   public synchronized Response poll(@HeaderParam(Constants.WAIT_HEADER) @DefaultValue("0") long wait,
                                     @PathParam("index") long index,
                                     @Context UriInfo info)
   {
      if (closed)
      {
         UriBuilder builder = info.getBaseUriBuilder();
         builder.path(info.getMatchedURIs().get(1))
                 .path("consume-next");
         String uri = builder.build().toString();

         // redirect to another consume-next

         return Response.status(307).location(URI.create(uri)).build();
      }
      return checkIndexAndPoll(wait, info, info.getMatchedURIs().get(1), index);
   }

   protected Response checkIndexAndPoll(long wait, UriInfo info, String basePath, long index)
   {
      ping();

      if (lastConsumed == null && index > 0)
      {
         return Response.status(412).entity("You are using an old consume-next link and are out of sync with the JMS session on the server").type("text/plain").build();
      }
      if (lastConsumed != null)
      {
         if (index == previousIndex)
         {
            String token = Long.toString(lastConsumed.getMessageID());
            return getMessageResponse(lastConsumed, info, basePath, token).build();
         }
         if (index != lastConsumed.getMessageID())
         {
            return Response.status(412).entity("You are using an old consume-next link and are out of sync with the JMS session on the server").type("text/plain").build();
         }
      }


      try
      {
         return pollWithIndex(wait, info, basePath, index);
      }
      finally
      {
         ping(); // ping again as we don't want wait time included in timeout.
      }
   }

   protected Response pollWithIndex(long wait, UriInfo info, String basePath, long index)
   {
      try
      {
         ClientMessage message = receive(wait);
         if (message == null)
         {
            Response.ResponseBuilder builder = Response.status(503).entity("Timed out waiting for message receive.").type("text/plain");
            setPollTimeoutLinks(info, basePath, builder, Long.toString(index));
            return builder.build();
         }
         previousIndex = index;
         lastConsumed = ConsumedMessage.createConsumedMessage(message);
         String token = Long.toString(lastConsumed.getMessageID());
         Response response = getMessageResponse(lastConsumed, info, basePath, token).build();
         if (autoAck) message.acknowledge();
         return response;
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   protected void createSession()
           throws HornetQException
   {
      session = factory.createSession(true, true, 0);
      if (selector == null)
      {
         consumer = session.createConsumer(destination);
      }
      else
      {
         consumer = session.createConsumer(destination, SelectorTranslator.convertToHornetQFilterString(selector));
      }
      session.start();
   }

   protected ClientMessage receiveFromConsumer(long timeoutSecs) throws Exception
   {
      if (timeoutSecs <= 0)
      {
         return consumer.receive(1);
      }
      else
      {
         return consumer.receive(timeoutSecs * 1000);
      }

   }

   protected ClientMessage receive(long timeoutSecs) throws Exception
   {
      return receiveFromConsumer(timeoutSecs);
   }

   protected void setPollTimeoutLinks(UriInfo info, String basePath, Response.ResponseBuilder builder, String index)
   {
      setSessionLink(builder, info, basePath);
      setConsumeNextLink(serviceManager.getLinkStrategy(), builder, info, basePath, index);
   }

   protected Response.ResponseBuilder getMessageResponse(ConsumedMessage msg, UriInfo info, String basePath, String index)
   {
      Response.ResponseBuilder responseBuilder = Response.ok();
      setMessageResponseLinks(info, basePath, responseBuilder, index);
      msg.build(responseBuilder);
      return responseBuilder;
   }

   protected void setMessageResponseLinks(UriInfo info, String basePath, Response.ResponseBuilder responseBuilder, String index)
   {
      setConsumeNextLink(serviceManager.getLinkStrategy(), responseBuilder, info, basePath, index);
      setSessionLink(responseBuilder, info, basePath);
   }

   public static void setConsumeNextLink(LinkStrategy linkStrategy, Response.ResponseBuilder response, UriInfo info, String basePath, String index)
   {
      if (index == null) throw new IllegalArgumentException("index cannot be null");
      UriBuilder builder = info.getBaseUriBuilder();
      builder.path(basePath)
              .path("consume-next" + index);
      String uri = builder.build().toString();
      linkStrategy.setLinkHeader(response, "consume-next", "consume-next", uri, MediaType.APPLICATION_FORM_URLENCODED);
   }

   public void setSessionLink(Response.ResponseBuilder response, UriInfo info, String basePath)
   {
      UriBuilder builder = info.getBaseUriBuilder();
      builder.path(basePath);
      String uri = builder.build().toString();
      serviceManager.getLinkStrategy().setLinkHeader(response, "consumer", "consumer", uri, MediaType.APPLICATION_XML);
   }
}