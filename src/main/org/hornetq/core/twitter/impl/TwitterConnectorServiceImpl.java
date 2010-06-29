/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.twitter.impl;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import twitter4j.*;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.HandleStatus;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.twitter.TwitterConstants;
import org.hornetq.core.twitter.TwitterConnectorService;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TwitterConnectorConfiguration;
import org.hornetq.core.filter.Filter;

/**
 * A TwitterConnectorServiceImpl
 *
 * @author <a href="tm.igarashi@gmail.com">Tomohisa Igarashi</a>
 * 
 */
public class TwitterConnectorServiceImpl implements TwitterConnectorService
{
   private static final Logger log = Logger.getLogger(TwitterConnectorServiceImpl.class);

   private volatile boolean isStarted = false;

   private final Configuration config;

   private final ScheduledExecutorService scheduledPool;
   
   private final StorageManager storageManager;
   
   private final PostOffice postOffice;
   
   private final List<IncomingTweetsHandler> incomingHandlers = new ArrayList<IncomingTweetsHandler>();

   private final HashMap<IncomingTweetsHandler, ScheduledFuture<?>> futureList = new HashMap<IncomingTweetsHandler,ScheduledFuture<?>>();

   private final List<OutgoingTweetsHandler> outgoingHandlers = new ArrayList<OutgoingTweetsHandler>();

   public TwitterConnectorServiceImpl(final Configuration config,
                                      final ScheduledExecutorService pool,
                                      final StorageManager storageManager,
                                      final PostOffice postOffice)
   {
      this.config = config;
      this.scheduledPool = pool;
      this.storageManager = storageManager;
      this.postOffice = postOffice;
   }

   public boolean isStarted()
   {
      return this.isStarted;
   }

   public synchronized void start() throws Exception
   {
      if (this.isStarted)
      {
         return;
      }

      for (TwitterConnectorConfiguration twitterConf : this.config.getTwitterConnectorConfigurations())
      {
         String connectorName = twitterConf.getConnectorName();

         if (twitterConf.isIncoming())
         {
            IncomingTweetsHandler incoming;
            try
            {
                incoming = new IncomingTweetsHandler(connectorName,
                                                    twitterConf.getUserName(),
                                                    twitterConf.getPassword(),
                                                    twitterConf.getQueueName(),
                                                    twitterConf.getIntervalSeconds(),
                                                    storageManager,
                                                    postOffice);
                incoming.initialize();
                ScheduledFuture<?> sf = this.scheduledPool.scheduleWithFixedDelay(incoming,
                                                                                  incoming.getIntervalSeconds(),
                                                                                  incoming.getIntervalSeconds(),
                                                                                  TimeUnit.SECONDS);
                this.futureList.put(incoming, sf);
                this.incomingHandlers.add(incoming);
            }
            catch(Exception e)
            {
               log.warn(connectorName + ": failed to initialize", e);
               continue;
            }
         }
         else
         {
            OutgoingTweetsHandler outgoing;
            try
            {
               outgoing = new OutgoingTweetsHandler(connectorName,
                                                   twitterConf.getUserName(),
                                                   twitterConf.getPassword(),
                                                   twitterConf.getQueueName(),
                                                   postOffice);
               outgoing.start();
               this.outgoingHandlers.add(outgoing);
            }
            catch(Exception e)
            {
               log.warn(connectorName + ": failed to initialize", e);
               continue;
            }
         }
         
         log.debug("Initialize twitter connector: [" + "connector-name=" +
                   connectorName +
                   ", username=" +
                   twitterConf.getUserName() +
                   ", queue-name=" +
                   twitterConf.getQueueName() +
                   ", interval-seconds=" +
                   twitterConf.getIntervalSeconds() +
                   "]");
      }

      this.isStarted = true;
      log.debug(this.getClass().getSimpleName() + " started");
   }

   public synchronized void stop() throws Exception
   {
      if (!this.isStarted)
      {
         return;
      }

      for (IncomingTweetsHandler in : this.incomingHandlers)
      {
         if (this.futureList.get(in).cancel(true))
         {
            this.futureList.remove(in);
            log.debug(in.getConnectorName() + ": stopped");
         }
         else
         {
            log.warn(in.getConnectorName() + ": stop failed");
         }
      }
      this.incomingHandlers.clear();

      for (OutgoingTweetsHandler out : this.outgoingHandlers)
      {
         try
         {
            out.shutdown();
         }
         catch(Exception e)
         {
            log.warn(e);
         }
      }
      this.outgoingHandlers.clear();

      this.isStarted = false;
      log.debug(this.getClass().getSimpleName() + " stopped");
   }

   public int getIncomingConnectorCount()
   {
      return incomingHandlers.size();
   }

   public int getOutgoingConnectorCount()
   {
      return outgoingHandlers.size();
   }

   /**
    * IncomingTweetsHandler consumes from twitter and forwards to the
    * configured HornetQ address.
    */
   private class IncomingTweetsHandler extends Thread
   {
      private final String connectorName;

      private final String userName;
      
      private final String password;
      
      private final String queueName;
      
      private final int intervalSeconds;

      private final StorageManager storageManager;

      private final PostOffice postOffice;

      private final Paging paging = new Paging();

      private Twitter twitter;

      public IncomingTweetsHandler(final String connectorName,
                                   final String userName,
                                   final String password,
                                   final String queueName,
                                   final int intervalSeconds,
                                   final StorageManager storageManager,
                                   final PostOffice postOffice) throws Exception
      {
         this.connectorName = connectorName;
         this.userName = userName;
         this.password = password;
         this.queueName = queueName;
         if (intervalSeconds > 0)
         {
            this.intervalSeconds = intervalSeconds;
         }
         else
         {
            this.intervalSeconds = TwitterConstants.DEFAULT_POLLING_INTERVAL_SECS;
         }
         this.storageManager = storageManager;
         this.postOffice = postOffice;
      }

      public void initialize() throws Exception
      {
         Binding b = postOffice.getBinding(new SimpleString(queueName));
         if(b == null)
         {
            throw new Exception(connectorName + ": queue " + queueName + " not found");
         }

         TwitterFactory tf = new TwitterFactory();
         this.twitter = tf.getInstance(userName, password);
         this.twitter.verifyCredentials();

         // getting latest ID
         this.paging.setCount(TwitterConstants.FIRST_ATTEMPT_PAGE_SIZE);
         ResponseList<Status> res = this.twitter.getHomeTimeline(paging);
         this.paging.setSinceId(res.get(0).getId());
         log.debug(connectorName + " initialise(): got latest ID: "+this.paging.getSinceId());

         // TODO make page size configurable
         this.paging.setCount(TwitterConstants.DEFAULT_PAGE_SIZE);
      }
      
      /**
       *  TODO streaming API support
       *  TODO rate limit support
       */
      public void run()
      {
         // Avoid cancelling the task with RuntimeException
         try
         {
            poll();
         }
         catch(Throwable t)
         {
            log.warn(connectorName, t);
         }
      }

      public int getIntervalSeconds()
      {
         return this.intervalSeconds;
      }

      public String getConnectorName()
      {
         return this.connectorName;
      }

      private void poll() throws Exception
      {
         // get new tweets         
         ResponseList<Status> res = this.twitter.getHomeTimeline(paging);

         if(res == null || res.size() == 0)
         {
            return;
         }
      
         for (int i = res.size() - 1; i >= 0; i--)
         {
            Status status = res.get(i);

            ServerMessage msg = new ServerMessageImpl(this.storageManager.generateUniqueID(),
                                                      TwitterConstants.INITIAL_MESSAGE_BUFFER_SIZE);
            msg.setAddress(new SimpleString(this.queueName));
            msg.setDurable(true);
            msg.encodeMessageIDToBuffer();

            putTweetIntoMessage(status, msg);

            this.postOffice.route(msg,false);
            log.debug(connectorName + ": routed: " + status.toString());
         }

         this.paging.setSinceId(res.get(0).getId());
         log.debug(connectorName + ": update latest ID: " + this.paging.getSinceId());
      }
      
      private void putTweetIntoMessage(final Status status, final ServerMessage msg)
      {
         msg.getBodyBuffer().writeString(status.getText());
         msg.putLongProperty(TwitterConstants.KEY_ID, status.getId());
         msg.putStringProperty(TwitterConstants.KEY_SOURCE, status.getSource());

         msg.putLongProperty(TwitterConstants.KEY_CREATED_AT, status.getCreatedAt().getTime());
         msg.putBooleanProperty(TwitterConstants.KEY_IS_TRUNCATED, status.isTruncated());
         msg.putLongProperty(TwitterConstants.KEY_IN_REPLY_TO_STATUS_ID, status.getInReplyToStatusId());
         msg.putIntProperty(TwitterConstants.KEY_IN_REPLY_TO_USER_ID, status.getInReplyToUserId());
         msg.putBooleanProperty(TwitterConstants.KEY_IS_FAVORITED, status.isFavorited());
         msg.putBooleanProperty(TwitterConstants.KEY_IS_RETWEET, status.isRetweet());
         msg.putObjectProperty(TwitterConstants.KEY_CONTRIBUTORS, status.getContributors());
         GeoLocation gl;
         if ((gl = status.getGeoLocation()) != null)
         {
            msg.putDoubleProperty(TwitterConstants.KEY_GEO_LOCATION_LATITUDE, gl.getLatitude());
            msg.putDoubleProperty(TwitterConstants.KEY_GEO_LOCATION_LONGITUDE, gl.getLongitude());
         }
         Place place;
         if ((place = status.getPlace()) != null)
         {
            msg.putStringProperty(TwitterConstants.KEY_PLACE_ID, place.getId());
         }
      }
   }

   /**
    * OutgoingTweetsHandler consumes from configured HornetQ address
    * and forwards to the twitter.
    */
   private class OutgoingTweetsHandler implements Consumer
   {
      private final String connectorName;
      
      private final String userName;
      
      private final String password;
      
      private final String queueName;
      
      private final PostOffice postOffice;
      
      private Twitter twitter = null;
      
      private Queue queue = null;

      private Filter filter = null;
      
      private boolean enabled = false;
      
      public OutgoingTweetsHandler(final String connectorName,
                                   final String userName,
                                   final String password,
                                   final String queueName,
                                   final PostOffice postOffice) throws Exception
      {
         this.connectorName = connectorName;
         this.userName = userName;
         this.password = password;
         this.queueName = queueName;
         this.postOffice = postOffice;
      }

      /**
       * TODO streaming API support 
       * TODO rate limit support
       */
      public synchronized void start() throws Exception
      {
         if(this.enabled)
         {
            return;
         }
         
         if(this.connectorName == null || this.connectorName.trim().equals(""))
         {
            throw new Exception("invalid connector name: " + this.connectorName);
         }
         
         if(this.queueName == null || this.queueName.trim().equals(""))
         {
            throw new Exception("invalid queue name: " + queueName);
         }
         
         SimpleString name = new SimpleString(this.queueName);
         Binding b = this.postOffice.getBinding(name);
         if(b == null)
         {
            throw new Exception(connectorName + ": queue " + queueName + " not found");
         }
         this.queue = (Queue)b.getBindable();

         TwitterFactory tf = new TwitterFactory();
         this.twitter = tf.getInstance(userName, password);
         this.twitter.verifyCredentials();
         // TODO make filter-string configurable
         // this.filter = FilterImpl.createFilter(filterString);
         this.filter = null;

         this.queue.addConsumer(this);

         this.queue.deliverAsync();
         this.enabled = true;
         log.debug(connectorName + ": started");
      }

      public synchronized void shutdown() throws Exception
      {
         if(!this.enabled)
         {
            return;
         }
         
         log.debug(connectorName + ": receive shutdown request");

         this.queue.removeConsumer(this);

         this.enabled = false;
         log.debug(connectorName + ": shutdown");
      }

      public Filter getFilter()
      {
         return filter;
      }

      public HandleStatus handle(final MessageReference ref) throws Exception
      {
         if (filter != null && !filter.match(ref.getMessage()))
         {
            return HandleStatus.NO_MATCH;
         }

         synchronized (this)
         {
            ref.handled();

            ServerMessage message = ref.getMessage();

            StatusUpdate status = new StatusUpdate(message.getBodyBuffer().readString());

            // set optional property
            
            if(message.containsProperty(TwitterConstants.KEY_IN_REPLY_TO_STATUS_ID))
            {
               status.setInReplyToStatusId(message.getLongProperty(TwitterConstants.KEY_IN_REPLY_TO_STATUS_ID));
            }
            
            if(message.containsProperty(TwitterConstants.KEY_GEO_LOCATION_LATITUDE))
            {
               double geolat = message.getDoubleProperty(TwitterConstants.KEY_GEO_LOCATION_LATITUDE);
               double geolong = message.getDoubleProperty(TwitterConstants.KEY_GEO_LOCATION_LONGITUDE);
               status.setLocation(new GeoLocation(geolat, geolong));
            }

            if(message.containsProperty(TwitterConstants.KEY_PLACE_ID))
            {
               status.setPlaceId(message.getStringProperty(TwitterConstants.KEY_PLACE_ID));
            }

            if(message.containsProperty(TwitterConstants.KEY_DISPLAY_COODINATES))
            {
               status.setDisplayCoordinates(message.getBooleanProperty(TwitterConstants.KEY_DISPLAY_COODINATES));
            }

            // send to Twitter
            try
            {
               this.twitter.updateStatus(status);
            }
            catch (TwitterException e)
            {
               if(e.getStatusCode() == 403 )
               {
                  // duplicated message
                  log.warn(connectorName + ": HTTP status code = 403: Ignore duplicated message");
                  queue.acknowledge(ref);
                  
                  return HandleStatus.HANDLED;
               }
               else
               {
                  throw e;
               }
            }

            queue.acknowledge(ref);
            log.debug(connectorName + ": forwarded to twitter: " + message.getMessageID());
            return HandleStatus.HANDLED;
         }
      }
   }
}
