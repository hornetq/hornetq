/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.core.deployers.impl;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.deployers.Deployer;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.logging.Logger;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class FileDeploymentManager implements Runnable, DeploymentManager
{
   private static final Logger log = Logger.getLogger(FileDeploymentManager.class);
     
   private final List<Deployer> deployers = new ArrayList<Deployer>();
   
   private final Map<URL, DeployInfo> deployed = new HashMap<URL, DeployInfo>();

   private ScheduledExecutorService scheduler;
   
   private boolean started;
   
   private final long period;
   
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
      
      scheduler = Executors.newSingleThreadScheduledExecutor();

      scheduler.scheduleWithFixedDelay(this, period, period, TimeUnit.MILLISECONDS);
      
      started = true;
   }

   public synchronized void stop()
   {
      if (!started)
      {
         return;
      }
      
      scheduler.shutdown();
      scheduler = null;
      deployers.clear();
      deployed.clear();   
      
      started = false;
   }

   /**
    * registers a Deployer object which will handle the deployment of URL's
    *
    * @param deployer The Deployer object
    * @throws Exception .
    */
   public synchronized void registerDeployer(final Deployer deployer) throws Exception
   {
      if (!started)
      {
         throw new IllegalStateException("Service is not started");
      }
      
      if (!deployers.contains(deployer))
      {
         deployers.add(deployer);
         
         String filename = deployer.getConfigFileName();
         
         log.info("the filename is " + filename);
         
         log.info(System.getProperty("java.class.path"));
         
         Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(filename);
         
         while (urls.hasMoreElements())
         {
            URL url = urls.nextElement();
            
            log.info("Got url " + url);
                                 
            try
            {
               log.info("Deploying " + deployer + " with url " + url);
               deployer.deploy(url);
            }
            catch (Exception e)
            {
               log.error("Error deploying " + url, e);
            }
            
            deployed.put(url, new DeployInfo(deployer, new File(url.getFile()).lastModified()));            
         }
      }      
      
      log.info("Done register");
   }

   public synchronized void unregisterDeployer(final Deployer deployer) throws Exception
   {
      if (!started)
      {
         throw new IllegalStateException("Service is not started");
      }
      
      if (deployers.remove(deployer))
      {
         Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(deployer.getConfigFileName());
         while (urls.hasMoreElements())
         {
            URL url = urls.nextElement();
            
            deployed.remove(url);
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
         throw new IllegalStateException("Service is not started");
      }
      
      try
      {
         for (Deployer deployer : deployers)
         {
            Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(deployer.getConfigFileName());

            while (urls.hasMoreElements())
            {
               URL url = urls.nextElement();
               
               DeployInfo info = deployed.get(url);
               
               long newLastModified = new File(url.getFile()).lastModified();
                  
               if (info == null)
               {                              
                  try
                  {
                     log.info("Deploying " + deployer + " with url " + url);
                     
                     deployer.deploy(url);
                     
                     deployed.put(url, new DeployInfo(deployer, new File(url.getFile()).lastModified()));
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
                     log.info("Redeploying " + deployer + " with url " + url);
                     
                     deployer.redeploy(url);
                     
                     deployed.put(url, new DeployInfo(deployer, new File(url.getFile()).lastModified()));
                  }
                  catch (Exception e)
                  {
                     log.error("Error redeploying " + url, e);
                  }
               }
            }         
         }
         
         for (Map.Entry<URL, DeployInfo> entry : deployed.entrySet())
         {
            if (!new File(entry.getKey().getFile()).exists())
            {
               try
               {
                  Deployer deployer = entry.getValue().deployer;
                  log.info("Undeploying " + deployer + " with url" + entry.getKey());
                  deployer.undeploy(entry.getKey());
                  
                  deployed.remove(entry.getKey());
               }
               catch (Exception e)
               {
                  log.error("Error undeploying " + entry.getKey(), e);
               }
            }
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
   
   public synchronized Map<URL, DeployInfo> getDeployed()
   {
      return deployed;
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

