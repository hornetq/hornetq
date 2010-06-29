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

package org.hornetq.core.twitter;

/**
 * A TwitterConstants
 *
 * @author <a href="tm.igarashi@gmail.com">Tomohisa Igarashi</a>
 */
public class TwitterConstants
{
   public static final String KEY_ID = "id";
   public static final String KEY_SOURCE = "source";
   public static final String KEY_CREATED_AT = "createdAt";
   public static final String KEY_IS_TRUNCATED = "isTruncated";
   public static final String KEY_IN_REPLY_TO_STATUS_ID = "inReplyToStatusId";
   public static final String KEY_IN_REPLY_TO_USER_ID = "inReplyToUserId";
   public static final String KEY_IN_REPLY_TO_SCREEN_NAME = "inReplyToScreenName";
   public static final String KEY_IS_FAVORITED = "isFavorited";
   public static final String KEY_IS_RETWEET = "isRetweet";
   public static final String KEY_CONTRIBUTORS = "contributors";
   public static final String KEY_GEO_LOCATION_LATITUDE = "geoLocation.latitude";
   public static final String KEY_GEO_LOCATION_LONGITUDE = "geoLocation.longitude";
   public static final String KEY_PLACE_ID = "place.id";
   public static final String KEY_DISPLAY_COODINATES = "displayCoodinates";
   
   public static final int DEFAULT_POLLING_INTERVAL_SECS = 10;
   public static final int DEFAULT_PAGE_SIZE = 100;
   public static final int FIRST_ATTEMPT_PAGE_SIZE = 1;
   public static final int START_SINCE_ID = 1;
   public static final int INITIAL_MESSAGE_BUFFER_SIZE = 50;
}
