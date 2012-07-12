package org.hornetq.rest.queue;

import org.hornetq.rest.queue.push.PushConsumerResource;

import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class QueueResource extends DestinationResource
{
   protected ConsumersResource consumers;
   protected PushConsumerResource pushConsumers;

   public void start() throws Exception
   {
   }

   public void stop()
   {
      consumers.stop();
      pushConsumers.stop();
      sender.cleanup();
   }

   @GET
   @Produces("application/xml")
   public Response get(@Context UriInfo uriInfo)
   {

      StringBuilder msg = new StringBuilder();
      msg.append("<queue>")
              .append("<name>").append(destination).append("</name>")
              .append("<atom:link rel=\"create\" href=\"").append(createSenderLink(uriInfo)).append("\"/>")
              .append("<atom:link rel=\"create-with-id\" href=\"").append(createSenderWithIdLink(uriInfo)).append("\"/>")
              .append("<atom:link rel=\"pull-consumers\" href=\"").append(createConsumersLink(uriInfo)).append("\"/>")
              .append("<atom:link rel=\"push-consumers\" href=\"").append(createPushConsumersLink(uriInfo)).append("\"/>")

         .append("</queue>");
      
      Response.ResponseBuilder builder = Response.ok(msg.toString());
      setSenderLink(builder, uriInfo);
      setSenderWithIdLink(builder, uriInfo);
      setConsumersLink(builder, uriInfo);
      setPushConsumersLink(builder, uriInfo);
      return builder.build();
   }

   @HEAD
   @Produces("application/xml")
   public Response head(@Context UriInfo uriInfo)
   {
      Response.ResponseBuilder builder = Response.ok();
      setSenderLink(builder, uriInfo);
      setSenderWithIdLink(builder, uriInfo);
      setConsumersLink(builder, uriInfo);
      setPushConsumersLink(builder, uriInfo);
      return builder.build();
   }

   protected void setSenderLink(Response.ResponseBuilder response, UriInfo info)
   {
      String uri = createSenderLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "create", "create", uri, null);
   }

   protected String createSenderLink(UriInfo info)
   {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("create");
      String uri = builder.build().toString();
      return uri;
   }

   protected void setSenderWithIdLink(Response.ResponseBuilder response, UriInfo info)
   {
      String uri = createSenderWithIdLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "create-with-id", "create-with-id", uri, null);
   }

   protected String createSenderWithIdLink(UriInfo info)
   {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("create");
      String uri = builder.build().toString();
      uri += "/{id}";
      return uri;
   }

   protected void setConsumersLink(Response.ResponseBuilder response, UriInfo info)
   {
      String uri = createConsumersLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "pull-consumers", "pull-consumers", uri, null);
   }

   protected String createConsumersLink(UriInfo info)
   {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("pull-consumers");
      String uri = builder.build().toString();
      return uri;
   }

   protected void setPushConsumersLink(Response.ResponseBuilder response, UriInfo info)
   {
      String uri = createPushConsumersLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "push-consumers", "push-consumers", uri, null);
   }

   protected String createPushConsumersLink(UriInfo info)
   {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("push-consumers");
      String uri = builder.build().toString();
      return uri;
   }


   public void setConsumers(ConsumersResource consumers)
   {
      this.consumers = consumers;
   }

   @Path("create")
   public PostMessage post() throws Exception
   {
      return sender;
   }


   @Path("pull-consumers")
   public ConsumersResource getConsumers()
   {
      return consumers;
   }

   public void setPushConsumers(PushConsumerResource pushConsumers)
   {
      this.pushConsumers = pushConsumers;
   }

   @Path("push-consumers")
   public PushConsumerResource getPushConsumers()
   {
      return pushConsumers;
   }
}