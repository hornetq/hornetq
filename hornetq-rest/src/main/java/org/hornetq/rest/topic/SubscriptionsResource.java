package org.hornetq.rest.topic;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.rest.HornetQRestLogger;
import org.hornetq.rest.queue.AcknowledgedQueueConsumer;
import org.hornetq.rest.queue.Acknowledgement;
import org.hornetq.rest.queue.DestinationServiceManager;
import org.hornetq.rest.queue.QueueConsumer;
import org.hornetq.rest.util.TimeoutTask;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class SubscriptionsResource implements TimeoutTask.Callback
{
   protected ConcurrentHashMap<String, QueueConsumer> queueConsumers = new ConcurrentHashMap<String, QueueConsumer>();
   protected ClientSessionFactory sessionFactory;
   protected String destination;
   protected final String startup = Long.toString(System.currentTimeMillis());
   protected AtomicLong sessionCounter = new AtomicLong(1);
   protected int consumerTimeoutSeconds;
   protected DestinationServiceManager serviceManager;

   public DestinationServiceManager getServiceManager()
   {
      return serviceManager;
   }

   public void setServiceManager(DestinationServiceManager serviceManager)
   {
      this.serviceManager = serviceManager;
   }

   public int getConsumerTimeoutSeconds()
   {
      return consumerTimeoutSeconds;
   }

   public void setConsumerTimeoutSeconds(int consumerTimeoutSeconds)
   {
      this.consumerTimeoutSeconds = consumerTimeoutSeconds;
   }

   public ClientSessionFactory getSessionFactory()
   {
      return sessionFactory;
   }

   public void setSessionFactory(ClientSessionFactory sessionFactory)
   {
      this.sessionFactory = sessionFactory;
   }

   public String getDestination()
   {
      return destination;
   }

   public void setDestination(String destination)
   {
      this.destination = destination;
   }


   public void testTimeout(String target)
   {
      QueueConsumer consumer = queueConsumers.get(target);
      Subscription subscription = (Subscription)consumer;
      if (consumer == null) return;
      synchronized (consumer)
      {
         if (System.currentTimeMillis() - consumer.getLastPingTime() > subscription.getTimeout())
         {
            HornetQRestLogger.LOGGER.shutdownRestSubscription(consumer.getId());
            consumer.shutdown();
            queueConsumers.remove(consumer.getId());
            serviceManager.getTimeoutTask().remove(consumer.getId());
            if (subscription.isDeleteWhenIdle()) deleteSubscriberQueue(consumer);
         }
      }
   }

   public void stop()
   {
      for (QueueConsumer consumer : queueConsumers.values())
      {
         consumer.shutdown();
         Subscription subscription = (Subscription) consumer;
         if (!subscription.isDurable())
         {
            deleteSubscriberQueue(consumer);
         }

      }
      queueConsumers.clear();
   }

   protected String generateSubscriptionName()
   {
      return startup + "-" + sessionCounter.getAndIncrement() + "-" + destination;
   }

   @POST
   public Response createSubscription(@FormParam("durable") @DefaultValue("false") boolean durable,
                                      @FormParam("autoAck") @DefaultValue("true") boolean autoAck,
                                      @FormParam("name") String subscriptionName,
                                      @FormParam("selector") String selector,
                                      @FormParam("delete-when-idle") Boolean destroyWhenIdle,
                                      @FormParam("idle-timeout") Long timeout,
                                      @Context UriInfo uriInfo)
   {
      HornetQRestLogger.LOGGER.debug("Handling POST request for \"" + uriInfo.getPath() + "\"");

      if (timeout == null)
         timeout = Long.valueOf(consumerTimeoutSeconds * 1000);
      boolean deleteWhenIdle = !durable; // default is true if non-durable
      if (destroyWhenIdle != null) deleteWhenIdle = destroyWhenIdle.booleanValue();

      if (subscriptionName != null)
      {
         // see if this is a reconnect
         QueueConsumer consumer = queueConsumers.get(subscriptionName);
         if (consumer != null)
         {
            boolean acked = consumer instanceof AcknowledgedSubscriptionResource;
            acked = !acked;
            if (acked != autoAck)
            {
               throw new WebApplicationException(
                       Response.status(412).entity("Consumer already exists and ack-modes don't match.").type("text/plain").build()
               );
            }
            Subscription sub = (Subscription) consumer;
            if (sub.isDurable() != durable)
            {
               throw new WebApplicationException(
                       Response.status(412).entity("Consumer already exists and durability doesn't match.").type("text/plain").build()
               );
            }
            Response.ResponseBuilder builder = Response.noContent();
            if (autoAck)
            {
               headAutoAckSubscriptionResponse(uriInfo, consumer, builder);
               consumer.setSessionLink(builder, uriInfo, uriInfo.getMatchedURIs().get(1) + "/auto-ack/" + consumer.getId());
            }
            else
            {
               headAcknowledgedConsumerResponse(uriInfo, (AcknowledgedQueueConsumer) consumer, builder);
               consumer.setSessionLink(builder, uriInfo, uriInfo.getMatchedURIs().get(1) + "/acknowledged/" + consumer.getId());
            }
            return builder.build();
         }
      }
      else
      {
         subscriptionName = generateSubscriptionName();
      }
      ClientSession session = null;
      try
      {
         // if this is not a reconnect, create the subscription queue
         if (!subscriptionExists(subscriptionName))
         {
            session = sessionFactory.createSession();

            if (durable)
            {
               session.createQueue(destination, subscriptionName, true);
            }
            else
            {
               session.createTemporaryQueue(destination, subscriptionName);
            }
         }
         QueueConsumer consumer = createConsumer(durable, autoAck, subscriptionName, selector, timeout, deleteWhenIdle);
         queueConsumers.put(consumer.getId(), consumer);
         serviceManager.getTimeoutTask().add(this, consumer.getId());

         UriBuilder location = uriInfo.getAbsolutePathBuilder();
         if (autoAck) location.path("auto-ack");
         else location.path("acknowledged");
         location.path(consumer.getId());
         Response.ResponseBuilder builder = Response.created(location.build());
         if (autoAck)
         {
            QueueConsumer.setConsumeNextLink(serviceManager.getLinkStrategy(), builder, uriInfo, uriInfo.getMatchedURIs().get(1) + "/auto-ack/" + consumer.getId(), "-1");
         }
         else
         {
            AcknowledgedQueueConsumer.setAcknowledgeNextLink(serviceManager.getLinkStrategy(), builder, uriInfo, uriInfo.getMatchedURIs().get(1) + "/acknowledged/" + consumer.getId(), "-1");

         }
         return builder.build();

      }
      catch (HornetQException e)
      {
         throw new RuntimeException(e);
      }
      finally
      {
         if (session != null)
         {
            try
            {
               session.close();
            }
            catch (HornetQException e)
            {
            }
         }
      }
   }

   protected QueueConsumer createConsumer(boolean durable, boolean autoAck, String subscriptionName, String selector, long timeout, boolean deleteWhenIdle)
           throws HornetQException
   {
      QueueConsumer consumer;
      if (autoAck)
      {
         SubscriptionResource subscription = new SubscriptionResource(sessionFactory, subscriptionName, subscriptionName, serviceManager, selector, durable, timeout);
         subscription.setDurable(durable);
         subscription.setDeleteWhenIdle(deleteWhenIdle);
         consumer = subscription;
      }
      else
      {
         AcknowledgedSubscriptionResource subscription = new AcknowledgedSubscriptionResource(sessionFactory, subscriptionName, subscriptionName, serviceManager, selector, durable, timeout);
         subscription.setDurable(durable);
         subscription.setDeleteWhenIdle(deleteWhenIdle);
         consumer = subscription;
      }
      return consumer;
   }

   @Path("auto-ack/{consumer-id}")
   @GET
   public Response getAutoAckSubscription(@PathParam("consumer-id") String consumerId,
                                          @Context UriInfo uriInfo) throws Exception
   {
      HornetQRestLogger.LOGGER.debug("Handling GET request for \"" + uriInfo.getPath() + "\"");

      return internalHeadAutoAckSubscription(uriInfo, consumerId);
   }

   @Path("auto-ack/{consumer-id}")
   @HEAD
   public Response headAutoAckSubscription(@PathParam("consumer-id") String consumerId,
                                           @Context UriInfo uriInfo) throws Exception
   {
      HornetQRestLogger.LOGGER.debug("Handling HEAD request for \"" + uriInfo.getPath() + "\"");

      return internalHeadAutoAckSubscription(uriInfo, consumerId);
   }

   private Response internalHeadAutoAckSubscription(UriInfo uriInfo, String consumerId)
   {
       QueueConsumer consumer = findAutoAckSubscription(consumerId);
       Response.ResponseBuilder builder = Response.noContent();
       headAutoAckSubscriptionResponse(uriInfo, consumer, builder);

       return builder.build();
   }

   private void headAutoAckSubscriptionResponse(UriInfo uriInfo, QueueConsumer consumer, Response.ResponseBuilder builder)
   {
      // we synchronize just in case a failed request is still processing
      synchronized (consumer)
      {
         QueueConsumer.setConsumeNextLink(serviceManager.getLinkStrategy(), builder, uriInfo, uriInfo.getMatchedURIs().get(1) + "/acknowledged/" + consumer.getId(), Long.toString(consumer.getConsumeIndex()));
      }
   }

   @Path("auto-ack/{subscription-id}")
   public QueueConsumer findAutoAckSubscription(
           @PathParam("subscription-id") String subscriptionId)
   {
      QueueConsumer consumer = queueConsumers.get(subscriptionId);
      if (consumer == null)
      {
         consumer = recreateTopicConsumer(subscriptionId, true);
      }
      return consumer;
   }

   @Path("acknowledged/{consumer-id}")
   @GET
   public Response getAcknowledgedConsumer(@PathParam("consumer-id") String consumerId,
                                           @Context UriInfo uriInfo) throws Exception
   {
      HornetQRestLogger.LOGGER.debug("Handling GET request for \"" + uriInfo.getPath() + "\"");

      return internalHeadAcknowledgedConsumer(uriInfo, consumerId);
   }

   @Path("acknowledged/{consumer-id}")
   @HEAD
   public Response headAcknowledgedConsumer(@PathParam("consumer-id") String consumerId,
                                            @Context UriInfo uriInfo) throws Exception
   {
      HornetQRestLogger.LOGGER.debug("Handling HEAD request for \"" + uriInfo.getPath() + "\"");

      return internalHeadAcknowledgedConsumer(uriInfo, consumerId);
   }

   private Response internalHeadAcknowledgedConsumer(UriInfo uriInfo, String consumerId)
   {
      AcknowledgedQueueConsumer consumer = (AcknowledgedQueueConsumer) findAcknoledgeSubscription(consumerId);
      Response.ResponseBuilder builder = Response.ok();
      headAcknowledgedConsumerResponse(uriInfo, consumer, builder);

      return builder.build();
   }

   private void headAcknowledgedConsumerResponse(UriInfo uriInfo, AcknowledgedQueueConsumer consumer, Response.ResponseBuilder builder)
   {
      // we synchronize just in case a failed request is still processing
      synchronized (consumer)
      {
         Acknowledgement ack = consumer.getAck();
         if (ack == null || ack.wasSet())
         {
            AcknowledgedQueueConsumer.setAcknowledgeNextLink(serviceManager.getLinkStrategy(), builder, uriInfo, uriInfo.getMatchedURIs().get(1) + "/acknowledged/" + consumer.getId(), Long.toString(consumer.getConsumeIndex()));
         }
         else
         {
            consumer.setAcknowledgementLink(builder, uriInfo, uriInfo.getMatchedURIs().get(1) + "/acknowledged/" + consumer.getId());
         }
      }
   }

   @Path("acknowledged/{subscription-id}")
   public QueueConsumer findAcknoledgeSubscription(
           @PathParam("subscription-id") String subscriptionId)
   {
      QueueConsumer consumer = queueConsumers.get(subscriptionId);
      if (consumer == null)
      {
         consumer = recreateTopicConsumer(subscriptionId, false);
      }
      return consumer;
   }

   private boolean subscriptionExists(String subscriptionId)
   {
      ClientSession session = null;
      try
      {
         session = sessionFactory.createSession();

         ClientSession.QueueQuery query = session.queueQuery(new SimpleString(subscriptionId));
         return query.isExists();
      }
      catch (HornetQException e)
      {
         throw new RuntimeException(e);
      }
      finally
      {
         if (session != null)
         {
            try
            {
               session.close();
            }
            catch (HornetQException e)
            {
            }
         }
      }
   }

   private QueueConsumer recreateTopicConsumer(String subscriptionId, boolean autoAck)
   {
      QueueConsumer consumer;
      if (subscriptionExists(subscriptionId))
      {
         QueueConsumer tmp = null;
         try
         {
            tmp = createConsumer(true, autoAck, subscriptionId, null, consumerTimeoutSeconds * 1000, false);
         }
         catch (HornetQException e)
         {
            throw new RuntimeException(e);
         }
         consumer = queueConsumers.putIfAbsent(subscriptionId, tmp);
         if (consumer == null)
         {
            consumer = tmp;
            serviceManager.getTimeoutTask().add(this, subscriptionId);
         }
         else
         {
            tmp.shutdown();
         }
      }
      else
      {
         throw new WebApplicationException(Response.status(405)
                 .entity("Failed to find subscriber " + subscriptionId + " you will have to reconnect")
                 .type("text/plain").build());
      }
      return consumer;
   }


   @Path("acknowledged/{subscription-id}")
   @DELETE
   public void deleteAckSubscription(@Context UriInfo uriInfo, @PathParam("subscription-id") String consumerId)
   {
      HornetQRestLogger.LOGGER.debug("Handling DELETE request for \"" + uriInfo.getPath() + "\"");

      internalDeleteSubscription(consumerId);
   }

   @Path("auto-ack/{subscription-id}")
   @DELETE
   public void deleteSubscription(@Context UriInfo uriInfo, @PathParam("subscription-id") String consumerId)
   {
      HornetQRestLogger.LOGGER.debug("Handling DELETE request for \"" + uriInfo.getPath() + "\"");

      internalDeleteSubscription(consumerId);
   }

   private void internalDeleteSubscription(String consumerId)
   {
      QueueConsumer consumer = queueConsumers.remove(consumerId);
      if (consumer == null)
      {
         String msg = "Failed to match a subscription to URL " + consumerId;
         throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
                  .entity(msg)
                  .type("text/plain").build());
      }
      consumer.shutdown();
      deleteSubscriberQueue(consumer);
   }

   private void deleteSubscriberQueue(QueueConsumer consumer)
   {
      String subscriptionName = consumer.getId();
      ClientSession session = null;
      try
      {
         session = sessionFactory.createSession();

         session.deleteQueue(subscriptionName);
      }
      catch (HornetQException e)
      {
      }
      finally
      {
         if (session != null)
         {
            try
            {
               session.close();
            }
            catch (HornetQException e)
            {
            }
         }
      }
   }
}