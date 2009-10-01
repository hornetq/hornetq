/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.core.deployers.impl;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.deployers.Deployer;
import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.Pair;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class FileDeploymentManager implements Runnable, DeploymentManager
{
   private static final Logger log = Logger.getLogger(FileDeploymentManager.class);

   private final List<Deployer> deployers = new ArrayList<Deployer>();

   private final Map<Pair<URL, Deployer>, DeployInfo> deployed = new HashMap<Pair<URL, Deployer>, DeployInfo>();

   private ScheduledExecutorService scheduler;

   private boolean started;

   private final long period;

   private ScheduledFuture<?> future;

   public FileDeploymentManager(final long period)
   {
      this.period = period;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      started = true;

      scheduler = Executors.newSingleThreadScheduledExecutor();

      future = scheduler.scheduleWithFixedDelay(this, period, period, TimeUnit.MILLISECONDS);
   }

   public synchronized void stop()
   {
      if (!started)
      {
         return;
      }

      started = false;

      if (future != null)
      {
         future.cancel(false);

         future = null;
      }

      scheduler.shutdown();

      scheduler = null;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   /**
    * registers a Deployer object which will handle the deployment of URL's
    *
    * @param deployer The Deployer object
    * @throws Exception .
    */
   public synchronized void registerDeployer(final Deployer deployer) throws Exception
   {    
      if (!deployers.contains(deployer))
      {
         deployers.add(deployer);

         String[] filenames = deployer.getConfigFileNames();

         for (String filename : filenames)
         {
            log.debug("the filename is " + filename);

            log.debug(System.getProperty("java.class.path"));

            Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(filename);

            while (urls.hasMoreElements())
            {
               URL url = urls.nextElement();

               log.debug("Got url " + url);

               try
               {
                  log.debug("Deploying " + url + " for " + deployer.getClass().getSimpleName());
                  deployer.deploy(url);
               }
               catch (Exception e)
               {
                  log.error("Error deploying " + url, e);
               }
               
               Pair<URL, Deployer> pair = new Pair<URL, Deployer>(url, deployer);

               deployed.put(pair, new DeployInfo(deployer, new File(url.getFile()).lastModified()));
            }
         }        
      }
   }

   public synchronized void unregisterDeployer(final Deployer deployer) throws Exception
   {
      if (deployers.remove(deployer))
      {
         String[] filenames = deployer.getConfigFileNames();
         for (String filename : filenames)
         {
            Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(filename);
            while (urls.hasMoreElements())
            {
               URL url = urls.nextElement();

               Pair<URL, Deployer> pair = new Pair<URL, Deployer>(url, deployer); 
               
               deployed.remove(pair);
            }
         }
      }
   }

   /**
    * called by the ExecutorService every n seconds
    */
   public synchronized void run()
   {
      if (!started)
      {
         return;
      }

      try
      {
         for (Deployer deployer : deployers)
         {   
            String[] filenames = deployer.getConfigFileNames();
            
            for (String filename : filenames)
            {
               Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(filename);

               while (urls.hasMoreElements())
               {
                  URL url = urls.nextElement();
                  
                  Pair<URL, Deployer> pair = new Pair<URL, Deployer>(url, deployer);

                  DeployInfo info = deployed.get(pair);

                  long newLastModified = new File(url.getFile()).lastModified();

                  if (info == null)
                  {
                     try
                     {
                        deployer.deploy(url);

                        deployed.put(pair, new DeployInfo(deployer, new File(url.getFile()).lastModified()));
                     }
                     catch (Exception e)
                     {
                        log.error("Error deploying " + url, e);
                     }
                  }
                  else if (newLastModified > info.lastModified)
                  {
                     try
                     {
                        deployer.redeploy(url);

                        deployed.put(pair, new DeployInfo(deployer, new File(url.getFile()).lastModified()));
                     }
                     catch (Exception e)
                     {
                        log.error("Error redeploying " + url, e);
                     }
                  }
               }
            }
         }
         List<Pair> toRemove = new ArrayList<Pair>();
         for (Map.Entry<Pair<URL, Deployer>, DeployInfo> entry : deployed.entrySet())
         {
            Pair<URL, Deployer> pair = entry.getKey();
            if (!fileExists(pair.a))
            {
               try
               {
                  Deployer deployer = entry.getValue().deployer;
                  log.info("Undeploying " + deployer + " with url " + pair.a);
                  deployer.undeploy(pair.a);
                  toRemove.add(pair);
               }
               catch (Exception e)
               {
                  log.error("Error undeploying " + pair.a, e);
               }
            }
         }
         for (Pair pair : toRemove)
         {
            deployed.remove(pair);  
         }
      }
      catch (Exception e)
      {
         log.warn("error scanning for URL's " + e);
      }
   }

   public synchronized List<Deployer> getDeployers()
   {
      return deployers;
   }

   public synchronized Map<Pair<URL, Deployer>, DeployInfo> getDeployed()
   {
      return deployed;
   }
   
   // Private -------------------------------------------------------
   
   /**
    * Checks if the URL is among the current thread context class loader's resources.
    * 
    * We do not check that the corresponding file exists using File.exists() directly as it would fail
    * in the case the resource is loaded from inside an EAR file (see https://jira.jboss.org/jira/browse/HORNETQ-122)
    */
   private boolean fileExists(URL resourceURL)
   {
      try
      {
         File f = new File(resourceURL.getPath());
         Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(f.getName());
         while (resources.hasMoreElements())
         {
            URL url = (URL)resources.nextElement();
            if (url.equals(resourceURL))
            {
               return true;
            }
         }
      }
      catch (Exception e)
      {
         return false;
      }
      return false;
   }

   // Inner classes -------------------------------------------------------------------------------------------

   public static class DeployInfo
   {
      public Deployer deployer;

      public long lastModified;

      DeployInfo(final Deployer deployer, final long lastModified)
      {
         this.deployer = deployer;
         this.lastModified = lastModified;
      }
   }
}
