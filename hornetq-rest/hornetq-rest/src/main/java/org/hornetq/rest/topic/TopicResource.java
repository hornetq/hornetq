package org.hornetq.rest.topic;

import org.hornetq.rest.queue.DestinationResource;
import org.hornetq.rest.queue.PostMessage;

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
public class TopicResource extends DestinationResource
{
   protected SubscriptionsResource subscriptions;
   protected PushSubscriptionsResource pushSubscriptions;

   public void start() throws Exception
   {
   }

   public void stop()
   {
      subscriptions.stop();
      pushSubscriptions.stop();
      sender.cleanup();
   }

   @GET
   @Produces("application/xml")
   public Response get(@Context UriInfo uriInfo)
   {

      StringBuilder msg = new StringBuilder();
      msg.append("<topic>")
              .append("<name>").append(destination).append("</name>")
              .append("<atom:link rel=\"create\" href=\"").append(createSenderLink(uriInfo)).append("\"/>")
              .append("<atom:link rel=\"create-with-id\" href=\"").append(createSenderWithIdLink(uriInfo)).append("\"/>")
              .append("<atom:link rel=\"pull-consumers\" href=\"").append(createSubscriptionsLink(uriInfo)).append("\"/>")
              .append("<atom:link rel=\"push-consumers\" href=\"").append(createPushSubscriptionsLink(uriInfo)).append("\"/>")

         .append("</topic>");

      Response.ResponseBuilder builder = Response.ok(msg.toString());
      setSenderLink(builder, uriInfo);
      setSenderWithIdLink(builder, uriInfo);
      setSubscriptionsLink(builder, uriInfo);
      setPushSubscriptionsLink(builder, uriInfo);
      return builder.build();
   }

   @HEAD
   @Produces("application/xml")
   public Response head(@Context UriInfo uriInfo)
   {
      Response.ResponseBuilder builder = Response.ok();
      setSenderLink(builder, uriInfo);
      setSenderWithIdLink(builder, uriInfo);
      setSubscriptionsLink(builder, uriInfo);
      setPushSubscriptionsLink(builder, uriInfo);
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

   protected void setSubscriptionsLink(Response.ResponseBuilder response, UriInfo info)
   {
      String uri = createSubscriptionsLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "pull-subscriptions", "pull-subscriptions", uri, null);
   }

   protected String createSubscriptionsLink(UriInfo info)
   {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("pull-subscriptions");
      String uri = builder.build().toString();
      return uri;
   }

   protected void setPushSubscriptionsLink(Response.ResponseBuilder response, UriInfo info)
   {
      String uri = createPushSubscriptionsLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "push-subscriptions", "push-subscriptions", uri, null);
   }

   protected String createPushSubscriptionsLink(UriInfo info)
   {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("push-subscriptions");
      String uri = builder.build().toString();
      return uri;
   }


   public void setSubscriptions(SubscriptionsResource subscriptions)
   {
      this.subscriptions = subscriptions;
   }

   @Path("create")
   public PostMessage post() throws Exception
   {
      return sender;
   }


   @Path("pull-subscriptions")
   public SubscriptionsResource getSubscriptions()
   {
      return subscriptions;
   }

   @Path("push-subscriptions")
   public PushSubscriptionsResource getPushSubscriptions()
   {
      return pushSubscriptions;
   }

   public void setPushSubscriptions(PushSubscriptionsResource pushSubscriptions)
   {
      this.pushSubscriptions = pushSubscriptions;
   }
}