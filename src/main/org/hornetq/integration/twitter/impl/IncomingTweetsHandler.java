/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.integration.twitter.impl;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.ConnectorService;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.integration.twitter.TwitterConstants;
import org.hornetq.utils.ConfigurationHelper;
import twitter4j.*;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * IncomingTweetsHandler consumes from twitter and forwards to the
 * configured HornetQ address.
 */
public class IncomingTweetsHandler implements ConnectorService
{
   private static final Logger log = Logger.getLogger(IncomingTweetsHandler.class);

   private final String connectorName;

   private final String userName;

   private final String password;

   private final String queueName;

   private final int intervalSeconds;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private  Paging paging;

   private Twitter twitter;

   private boolean isStarted = false;

   private final ScheduledExecutorService scheduledPool;

   private ScheduledFuture scheduledFuture;

   public IncomingTweetsHandler(final String connectorName, 
                                final Map<String, Object> configuration,
                                final StorageManager storageManager,
                                final PostOffice postOffice,
                                final ScheduledExecutorService scheduledThreadPool)
   {
      this.connectorName = connectorName;
      this.userName = ConfigurationHelper.getStringProperty(TwitterConstants.USER_NAME, null, configuration);
      this.password = ConfigurationHelper.getStringProperty(TwitterConstants.PASSWORD, null, configuration);
      this.queueName = ConfigurationHelper.getStringProperty(TwitterConstants.QUEUE_NAME, null, configuration);
      Integer intervalSeconds = ConfigurationHelper.getIntProperty(TwitterConstants.INCOMING_INTERVAL, 0, configuration);
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
      this.scheduledPool = scheduledThreadPool;
   }

   public void start() throws Exception
   {
      Binding b = postOffice.getBinding(new SimpleString(queueName));
      if (b == null)
      {
         throw new Exception(connectorName + ": queue " + queueName + " not found");
      }

      paging = new Paging();
      TwitterFactory tf = new TwitterFactory();
      this.twitter = tf.getInstance(userName, password);
      this.twitter.verifyCredentials();

      // getting latest ID
      this.paging.setCount(TwitterConstants.FIRST_ATTEMPT_PAGE_SIZE);
      ResponseList<Status> res = this.twitter.getHomeTimeline(paging);
      this.paging.setSinceId(res.get(0).getId());
      log.debug(connectorName + " initialise(): got latest ID: " + this.paging.getSinceId());

      // TODO make page size configurable
      this.paging.setCount(TwitterConstants.DEFAULT_PAGE_SIZE);

      scheduledFuture = this.scheduledPool.scheduleWithFixedDelay(new TweetsRunnable(),
                                                intervalSeconds,
                                                intervalSeconds,
                                                TimeUnit.SECONDS);
      isStarted = true;
   }

   public void stop() throws Exception
   {
      if(!isStarted)
      {
         return;
      }
      scheduledFuture.cancel(true);
      paging = null;
      isStarted = false;
   }

   public boolean isStarted()
   {
      return isStarted;
   }

   private void poll() throws Exception
   {
      // get new tweets
      ResponseList<Status> res = this.twitter.getHomeTimeline(paging);

      if (res == null || res.size() == 0)
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

         this.postOffice.route(msg, false);
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

   public String getName()
   {
      return connectorName;
   }

   private final class TweetsRunnable implements Runnable
   {
      /**
       * TODO streaming API support
       * TODO rate limit support
       */
      public void run()
      {
         // Avoid cancelling the task with RuntimeException
         try
         {
            poll();
         }
         catch (Throwable t)
         {
            log.warn(connectorName, t);
         }
      }
   }
}
