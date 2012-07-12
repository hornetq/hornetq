package org.hornetq.rest.queue;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;

import javax.ws.rs.POST;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;

/**
 * Implements simple "create" link.  Returns 201 with Location of created resource as per HTTP
 *
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class PostMessageDupsOk extends PostMessage
{

   public void publish(HttpHeaders headers, byte[] body, boolean durable,
                       Long ttl,
                       Long expiration,
                       Integer priority) throws Exception
   {
      Pooled pooled = getPooled();
      try
      {
         ClientProducer producer = pooled.producer;
         ClientMessage message = createHornetQMessage(headers, body, durable, ttl, expiration, priority, pooled.session);
         producer.send(message);
         pool.add(pooled);
      }
      catch (Exception ex)
      {
         try
         {
            pooled.session.close();
         }
         catch (HornetQException e)
         {
         }
         addPooled();
         throw ex;
      }
   }

   @POST
   public Response create(@Context HttpHeaders headers,
                          @QueryParam("durable") Boolean durable,
                          @QueryParam("ttl") Long ttl,
                          @QueryParam("expiration") Long expiration,
                          @QueryParam("priority") Integer priority,
                          @Context UriInfo uriInfo,
                          byte[] body)
   {
      try
      {
         boolean isDurable = defaultDurable;
         if (durable != null)
         {
            isDurable = durable.booleanValue();
         }
         publish(headers, body, isDurable, ttl, expiration, priority);
      }
      catch (Exception e)
      {
         Response error = Response.serverError()
                 .entity("Problem posting message: " + e.getMessage())
                 .type("text/plain")
                 .build();
         throw new WebApplicationException(e, error);
      }
      Response.ResponseBuilder builder = Response.status(201);
      UriBuilder nextBuilder = uriInfo.getAbsolutePathBuilder();
      URI next = nextBuilder.build();
      serviceManager.getLinkStrategy().setLinkHeader(builder, "create-next", "create-next", next.toString(), "*/*");
      return builder.build();
   }

}