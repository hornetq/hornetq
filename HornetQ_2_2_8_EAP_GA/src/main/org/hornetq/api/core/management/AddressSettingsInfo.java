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

package org.hornetq.api.core.management;

import org.hornetq.utils.json.JSONObject;

/**
 * A AddressSettingsInfo
 *
 * @author jmesnil
 *
 *
 */
public class AddressSettingsInfo
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String addressFullMessagePolicy;

   private long maxSizeBytes;

   private int pageSizeBytes;
   
   private int pageCacheMaxSize;

   private int maxDeliveryAttempts;

   private long redeliveryDelay;

   private String deadLetterAddress;

   private String expiryAddress;

   private boolean lastValueQueue;

   private long redistributionDelay;

   private boolean sendToDLAOnNoRoute;

   // Static --------------------------------------------------------

   public static final AddressSettingsInfo from(final String jsonString) throws Exception
   {
      JSONObject object = new JSONObject(jsonString);
      return new AddressSettingsInfo(object.getString("addressFullMessagePolicy"),
                                     object.getLong("maxSizeBytes"),
                                     object.getInt("pageSizeBytes"),
                                     object.getInt("pageCacheMaxSize"),
                                     object.getInt("maxDeliveryAttempts"),
                                     object.getLong("redeliveryDelay"),
                                     object.getString("DLA"),
                                     object.getString("expiryAddress"),
                                     object.getBoolean("lastValueQueue"),
                                     object.getLong("redistributionDelay"),
                                     object.getBoolean("sendToDLAOnNoRoute"));
   }

   // Constructors --------------------------------------------------

   public AddressSettingsInfo(String addressFullMessagePolicy,
                              long maxSizeBytes,
                              int pageSizeBytes,
                              int pageCacheMaxSize,
                              int maxDeliveryAttempts,
                              long redeliveryDelay,
                              String deadLetterAddress,
                              String expiryAddress,
                              boolean lastValueQueue,
                              long redistributionDelay,
                              boolean sendToDLAOnNoRoute)
   {
      this.addressFullMessagePolicy = addressFullMessagePolicy;
      this.maxSizeBytes = maxSizeBytes;
      this.pageSizeBytes = pageSizeBytes;
      this.pageCacheMaxSize = pageCacheMaxSize;
      this.maxDeliveryAttempts = maxDeliveryAttempts;
      this.redeliveryDelay = redeliveryDelay;
      this.deadLetterAddress = deadLetterAddress;
      this.expiryAddress = expiryAddress;
      this.lastValueQueue = lastValueQueue;
      this.redistributionDelay = redistributionDelay;
      this.sendToDLAOnNoRoute = sendToDLAOnNoRoute;
   }

   // Public --------------------------------------------------------

   public int getPageCacheMaxSize()
   {
      return pageCacheMaxSize;
   }

   public void setPageCacheMaxSize(int pageCacheMaxSize)
   {
      this.pageCacheMaxSize = pageCacheMaxSize;
   }

   public String getAddressFullMessagePolicy()
   {
      return addressFullMessagePolicy;
   }

   public long getMaxSizeBytes()
   {
      return maxSizeBytes;
   }

   public int getPageSizeBytes()
   {
      return pageSizeBytes;
   }

   public int getMaxDeliveryAttempts()
   {
      return maxDeliveryAttempts;
   }

   public long getRedeliveryDelay()
   {
      return redeliveryDelay;
   }

   public String getDeadLetterAddress()
   {
      return deadLetterAddress;
   }

   public String getExpiryAddress()
   {
      return expiryAddress;
   }

   public boolean isLastValueQueue()
   {
      return lastValueQueue;
   }

   public long getRedistributionDelay()
   {
      return redistributionDelay;
   }

   public boolean isSendToDLAOnNoRoute()
   {
      return sendToDLAOnNoRoute;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
