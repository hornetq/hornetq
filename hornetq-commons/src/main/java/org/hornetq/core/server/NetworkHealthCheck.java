/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hornetq.core.server;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jboss.logging.Logger;

/** This will use {@link InetAddress#isReachable(int)} to determine if the network is alive.
 *  It will have a set of addresses, and if any address is reached the network will be considered alive. */
public class NetworkHealthCheck extends ActiveMQScheduledComponent
{

   private static final String PREFIX = "org.hornetq.networkhealthcheck.";

   public static final String NIC_NAME_SYSTEM_PROP = PREFIX + "";
   public static final String CHECK_PERIOD_SYSTEM_PROP = PREFIX + "checkPeriod";
   public static final Long DEFAULT_CHECK_PERIOD = 5000L;
   public static final String TIMEOUT_SYSTEM_PROP = PREFIX + "timeout";
   public static final Integer DEFAULT_TIMEOUT = 5;
   public static final String ADDRESS_LIST_SYSTEM_PROP = PREFIX + "addressList";

   private static final Logger logger = Logger.getLogger(NetworkHealthCheck.class);

   private final Set<HornetQComponent> componentList = new HashSet<HornetQComponent>();
   private final Set<InetAddress> addresses = new HashSet<InetAddress>();
   private final Set<URL> urls = new HashSet<URL>();
   private final NetworkInterface networkInterface;


   /**
    * The timeout to be used on isReachable
    */
   private final int networkTimeout;

   public NetworkHealthCheck(ScheduledExecutorService scheduledExecutorService, Executor executor) throws Exception
   {
      this(System.getProperty(NetworkHealthCheck.NIC_NAME_SYSTEM_PROP, null),
           scheduledExecutorService, executor,
           Long.parseLong(System.getProperty(NetworkHealthCheck.CHECK_PERIOD_SYSTEM_PROP,
                                             NetworkHealthCheck.DEFAULT_CHECK_PERIOD.toString())),
           Integer.parseInt(System.getProperty(NetworkHealthCheck.TIMEOUT_SYSTEM_PROP,
                                               NetworkHealthCheck.DEFAULT_TIMEOUT.toString())));

   }

   public NetworkHealthCheck(String nicName,
                             ScheduledExecutorService scheduledExecutorService,
                             Executor executor,
                             long checkPeriod,
                             int networkTimeout) throws Exception
   {
      super(scheduledExecutorService, executor, checkPeriod, TimeUnit.MILLISECONDS, false);
      this.networkTimeout = networkTimeout;
      if (nicName != null)
      {
         this.networkInterface = NetworkInterface.getByName(nicName);
      }
      else
      {
         this.networkInterface = null;
      }


      String addressListProperty = System.getProperty(NetworkHealthCheck.ADDRESS_LIST_SYSTEM_PROP);

      if (addressListProperty != null)
      {
         String[] addresses = addressListProperty.split(",");

         for (String address: addresses)
         {
            this.addAddress(InetAddress.getByName(address));
         }
      }

   }

   public synchronized void addComponent(HornetQComponent component)
   {
      componentList.add(component);
   }

   public synchronized void clearComponents()
   {
      componentList.clear();
   }

   public synchronized void addAddress(InetAddress address)
   {
      if (!check(address))
      {
         logger.warn("Ping Address " + address + " wasn't reacheable");
      }
      addresses.add(address);
   }

   public synchronized void removeAddress(InetAddress address)
   {
      addresses.remove(address);
   }

   public synchronized void clearAddresses()
   {
      addresses.clear();
   }

   public synchronized void addURL(URL url)
   {
      if (!check(url))
      {
         logger.warn("Ping url " + url + " wasn't reacheable");
      }
      urls.add(url);
   }

   public synchronized void removeURL(URL url)
   {
      urls.remove(url);
   }

   public synchronized void clearURL()
   {
      urls.clear();
   }

   @Override
   public synchronized void run()
   {
      if (addresses.isEmpty() && urls.isEmpty())
      {
         return;
      }

      boolean healthy = check();


      if (healthy)
      {
         for (HornetQComponent component : componentList)
         {
            if (!component.isStarted())
            {
               try
               {
                  logger.info("Network is healthy, starting service " + component);
                  component.start();
               }
               catch (Exception e)
               {
                  logger.warn("Error starting component " + component, e);
               }
            }
         }
      }
      else
      {
         for (HornetQComponent component : componentList)
         {
            if (component.isStarted())
            {
               try
               {
                  logger.info("Network is unhealthy, stopping service " + component);
                  component.stop();
               }
               catch (Exception e)
               {
                  logger.warn("Error stopping component " + component, e);
               }
            }
         }
      }


   }

   public boolean check()
   {

      boolean isEmpty = true;
      for (InetAddress address : addresses)
      {
         isEmpty = false;
         if (check(address))
         {
            return true;
         }
      }


      for (URL url : urls)
      {
         isEmpty = false;
         if (check(url))
         {
            return true;
         }
      }

      // This should return true if no checks were done, on this case it's empty
      // This is tested by {@link NetworkHe
      return isEmpty;
   }

   public boolean check(InetAddress address)
   {
      try
      {
         if (address.isReachable(networkInterface, 0, networkTimeout))
         {
            if (logger.isTraceEnabled())
            {
               logger.tracef(address + " OK");
            }
            return true;
         }
         else
         {
            if (logger.isTraceEnabled())
            {
               logger.tracef(address + " can't be reached");
            }
            return false;
         }
      }
      catch (Exception e)
      {
         logger.warn(e.getMessage(), e);
         return false;
      }
   }

   public boolean check(URL url)
   {
      try
      {
         URLConnection connection = url.openConnection();
         InputStream is = connection.getInputStream();
         is.close();
         return true;
      }
      catch (Exception e)
      {
         logger.debug(e.getMessage(), e);
         return false;
      }
   }
}
