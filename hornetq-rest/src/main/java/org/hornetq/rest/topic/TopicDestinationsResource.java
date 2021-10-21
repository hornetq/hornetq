/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.rest.topic;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.client.HornetQTopic;
import org.hornetq.jms.server.config.TopicConfiguration;
import org.hornetq.jms.server.impl.JMSServerConfigParserImpl;
import org.hornetq.rest.HornetQRestLogger;
import org.hornetq.rest.queue.DestinationSettings;
import org.hornetq.rest.queue.PostMessage;
import org.hornetq.rest.queue.PostMessageDupsOk;
import org.hornetq.rest.queue.PostMessageNoDups;
import org.w3c.dom.Document;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
@Path("/topics")
public class TopicDestinationsResource
{
   private Map<String, TopicResource> topics = new ConcurrentHashMap<String, TopicResource>();
   private TopicServiceManager manager;

   public TopicDestinationsResource(TopicServiceManager manager)
   {
      this.manager = manager;
   }

   @POST
   @Consumes("application/hornetq.jms.topic+xml")
   public Response createJmsQueue(@Context UriInfo uriInfo, Document document)
   {
      HornetQRestLogger.LOGGER.debug("Handling POST request for \"" + uriInfo.getPath() + "\"");

      try
      {
         JMSServerConfigParserImpl parser = new JMSServerConfigParserImpl();
         TopicConfiguration topic = parser.parseTopicConfiguration(document.getDocumentElement());
         HornetQTopic hqTopic = HornetQDestination.createTopic(topic.getName());
         String topicName = hqTopic.getAddress();
         ClientSession session = manager.getSessionFactory().createSession(false, false, false);
         try
         {

            ClientSession.QueueQuery query = session.queueQuery(new SimpleString(topicName));
            if (!query.isExists())
            {
               session.createQueue(topicName, topicName, "__HQX=-1", true);

            }
            else
            {
               throw new WebApplicationException(Response.status(412).type("text/plain").entity("Queue already exists.").build());
            }
         }
         finally
         {
            try
            {
               session.close();
            }
            catch (Exception ignored)
            {
            }
         }
         if (topic.getBindings() != null && topic.getBindings().length > 0 && manager.getRegistry() != null)
         {
            for (String binding : topic.getBindings())
            {
               manager.getRegistry().bind(binding, hqTopic);
            }
         }
         URI uri = uriInfo.getRequestUriBuilder().path(topicName).build();
         return Response.created(uri).build();
      }
      catch (Exception e)
      {
         if (e instanceof WebApplicationException) throw (WebApplicationException) e;
         throw new WebApplicationException(e, Response.serverError().type("text/plain").entity("Failed to create queue.").build());
      }
   }


   @Path("/{topic-name}")
   public TopicResource findTopic(@PathParam("topic-name") String name) throws Exception
   {
      TopicResource topic = topics.get(name);
      if (topic == null)
      {
         ClientSession session = manager.getSessionFactory().createSession(false, false, false);
         try
         {
            ClientSession.QueueQuery query = session.queueQuery(new SimpleString(name));
            if (!query.isExists())
            {
               System.err.println("Topic '" + name + "' does not exist");
               throw new WebApplicationException(Response.status(404).type("text/plain").entity("Topic '" + name + "' does not exist").build());
            }
            DestinationSettings queueSettings = manager.getDefaultSettings();
            boolean defaultDurable = queueSettings.isDurableSend() || query.isDurable();

            topic = createTopicResource(name, defaultDurable, queueSettings.getConsumerSessionTimeoutSeconds(), queueSettings.isDuplicatesAllowed());
         }
         finally
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
      return topic;
   }

   public Map<String, TopicResource> getTopics()
   {
      return topics;
   }

   public TopicResource createTopicResource(String topicName, boolean defaultDurable, int timeoutSeconds, boolean duplicates) throws Exception
   {
      TopicResource topicResource = new TopicResource();
      topicResource.setTopicDestinationsResource(this);
      topicResource.setDestination(topicName);
      topicResource.setServiceManager(manager);
      SubscriptionsResource subscriptionsResource = new SubscriptionsResource();
      topicResource.setSubscriptions(subscriptionsResource);
      subscriptionsResource.setConsumerTimeoutSeconds(timeoutSeconds);
      subscriptionsResource.setServiceManager(manager);

      subscriptionsResource.setDestination(topicName);
      subscriptionsResource.setSessionFactory(manager.getConsumerSessionFactory());
      PushSubscriptionsResource push = new PushSubscriptionsResource(manager.getJmsOptions());
      push.setDestination(topicName);
      push.setSessionFactory(manager.getConsumerSessionFactory());
      topicResource.setPushSubscriptions(push);

      PostMessage sender = null;
      if (duplicates)
      {
         sender = new PostMessageDupsOk();
      }
      else
      {
         sender = new PostMessageNoDups();
      }
      sender.setDefaultDurable(defaultDurable);
      sender.setDestination(topicName);
      sender.setSessionFactory(manager.getSessionFactory());
      sender.setPoolSize(manager.getProducerPoolSize());
      sender.setProducerTimeToLive(manager.getProducerTimeToLive());
      sender.setServiceManager(manager);
      sender.init();
      topicResource.setSender(sender);

      if (manager.getPushStore() != null)
      {
         push.setPushStore(manager.getPushStore());
         List<PushTopicRegistration> regs = manager.getPushStore().getByTopic(topicName);
         for (PushTopicRegistration reg : regs)
         {
            push.addRegistration(reg);
         }
      }

      getTopics().put(topicName, topicResource);
      topicResource.start();
      return topicResource;
   }
}