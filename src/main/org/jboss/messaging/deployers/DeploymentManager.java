/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.deployers;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jboss.logging.Logger;

/**
 * This class manages any configuration files available. It will notify any deployers registered with it on changes.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class DeploymentManager implements Runnable
{
   private static final Logger log = Logger.getLogger(DeploymentManager.class);
   //use the singleton pattern we only need one of these
   private static DeploymentManager singleton;
   //these are the list of deployers, typically destination and connection factory.
   private static ArrayList<Deployable> deployables = new ArrayList<Deployable>();
   //any config files deployed and the time they were deployed
   private static HashMap<URL, Long> deployed = new HashMap<URL, Long>();
   // the list of URL's to deploy
   private static ArrayList<URL> toDeploy = new ArrayList<URL>();
   //the list of URL's to undeploy if removed
   private static ArrayList<URL> toUndeploy = new ArrayList<URL>();
   //the list of URL's to redeploy if changed
   private static ArrayList<URL> toRedeploy = new ArrayList<URL>();
   private static ScheduledExecutorService scheduler;

   //we want to use a singleton
   private DeploymentManager()
   {

   }

   public static DeploymentManager getInstance() throws Exception
   {
      //if the first time initialise and get the URL's to deploy
      if (singleton == null)
      {
         singleton = new DeploymentManager();
         //get the URL's to deploy and add them to the list with the timestamp
         Collection<ConfigurationURL> configurations = getConfigurations();
         for (ConfigurationURL configuration : configurations)
         {
            Iterator<URL> urls = configuration.getUrls();
            while (urls.hasNext())
            {
               URL url = urls.next();
               log.info(new StringBuilder("adding url ").append(url).append(" to be deployed"));
               deployed.put(url, new File(url.getFile()).lastModified());
            }
         }

         // Get the scheduler
         scheduler = Executors.newSingleThreadScheduledExecutor();

         scheduler.scheduleAtFixedRate(singleton, 10, 5, TimeUnit.SECONDS);
      }
      return singleton;
   }

   /**
    * will return any resources available
    *
    * @return a set of configurationUrls
    * @throws IOException .
    */
   private static Collection<ConfigurationURL> getConfigurations() throws IOException
   {
      HashMap<String, ConfigurationURL> configurations = new HashMap<String, ConfigurationURL>();
      for (Deployable deployable : deployables)
      {
         Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(deployable.getConfigFileName());

         if(!configurations.keySet().contains(deployable.getConfigFileName()))
         {
            ConfigurationURL conf = new ConfigurationURL(urls, deployable.getConfigFileName());
            configurations.put(deployable.getConfigFileName(), conf);
         }
         else
         {
            configurations.get(deployable.getConfigFileName()).add(urls);
         }
      }
      return configurations.values();
   }

   /**
    * registers a deployable object which will handle the deployment of URL's
    *
    * @param deployable The deployable object
    * @throws Exception .
    */
   public void registerDeployable(Deployable deployable) throws Exception
   {
      synchronized (this)
      {
         deployables.add(deployable);
         Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(deployable.getConfigFileName());
         while (urls.hasMoreElements())
         {
            URL url = urls.nextElement();
            if (!deployed.keySet().contains(url))
            {
               deployed.put(url, new File(url.getFile()).lastModified());
            }
            try
            {
               log.info(new StringBuilder("Deploying ").append(deployable).append(" with url").append(url));
               deployable.deploy(url);

            }
            catch (Exception e)
            {
               log.error(new StringBuilder("Error deploying ").append(url), e);
            }
         }
      }
   }

   public void unregisterDeployable(Deployable deployable)
   {
      deployables.remove(deployable);
      if(deployables.size() == 0)
      {
         if (scheduler != null)
         {
            scheduler.shutdown();
            scheduler = null;
         }
      }
   }
   /**
    * called by the ExecutorService every n seconds
    */
   public void run()
   {
      synchronized (this)
      {
         try
         {
            scan();
         }
         catch (Exception e)
         {
            log.warn("error scanning for URL's " + e);
         }
      }
   }

   /**
    * scans for changes to any of the configuration files registered
    *
    * @throws Exception .
    */
   private void scan() throws Exception
   {
      Collection<ConfigurationURL> configurations = getConfigurations();
      for (ConfigurationURL configuration : configurations)
      {
         Iterator<URL> urls = configuration.getUrls();
         while (urls.hasNext())
         {
            URL url = urls.next();
            if (!deployed.keySet().contains(url))
            {
               log.info(new StringBuilder("adding url ").append(url).append(" to be deployed"));
               toDeploy.add(url);
            }
            else if (new File(url.getFile()).lastModified() > deployed.get(url))
            {
               log.info(new StringBuilder("adding url ").append(url).append(" to be redeployed"));
               toRedeploy.add(url);
            }
         }
         for (URL url : deployed.keySet())
         {
            if (!new File(url.getFile()).exists())
            {
               log.info(new StringBuilder("adding url ").append(url).append(" to be undeployed"));
               toUndeploy.add(url);
            }
         }
      }

      for (URL url : toDeploy)
      {
         deploy(url);
      }
      for (URL url : toRedeploy)
      {
         redeploy(url);
      }
      for (URL url : toUndeploy)
      {
         undeploy(url);
      }
      toRedeploy.clear();
      toUndeploy.clear();
      toDeploy.clear();
   }

   /**
    * undeploys a url, delegates to appropiate registered deployables
    * @param url the url to undeploy
    */
   private void undeploy(URL url)
   {
      deployed.remove(url);

      for (Deployable deployable : deployables)
      {
         try
         {
            log.info(new StringBuilder("Undeploying ").append(deployable).append(" with url").append(url));
            deployable.undeploy(url);
         }
         catch (Exception e)
         {
            log.error(new StringBuilder("Error undeploying ").append(url), e);
         }
      }
   }

    /**
    * redeploys a url, delegates to appropiate registered deployables
    * @param url the url to redeploy
    */
   private void redeploy(URL url)
   {
      deployed.put(url, new File(url.getFile()).lastModified());
      for (Deployable deployable : deployables)
      {
         try
         {
            log.info(new StringBuilder("Redeploying ").append(deployable).append(" with url").append(url));
            deployable.redeploy(url);
         }
         catch (Exception e)
         {
            log.error(new StringBuilder("Error redeploying ").append(url), e);
         }
      }
   }

    /**
    * deploys a url, delegates to appropiate registered deployables 
    * @param url the url to deploy
    * @throws Exception .
    */
   private void deploy(URL url)
           throws Exception
   {
      deployed.put(url, new File(url.getFile()).lastModified());
      for (Deployable deployable : deployables)
      {
         try
         {
            log.info(new StringBuilder("Deploying ").append(deployable).append(" with url").append(url));
            deployable.deploy(url);
         }
         catch (Exception e)
         {
            log.error(new StringBuilder("Error deploying ").append(url), e);
         }
      }
   }

   static class ConfigurationURL
   {
      private ArrayList<URL> urls = new ArrayList<URL>();
      private String configFileName;

      public ConfigurationURL(Enumeration<URL> urls, String configFileName)
      {
         while (urls.hasMoreElements())
         {
            URL url = urls.nextElement();
            this.urls.add(url);
         }
         this.configFileName = configFileName;
      }

      public Iterator<URL> getUrls()
      {
         return urls.iterator();
      }

      public String getConfigFileName()
      {
         return configFileName;
      }

      public void add(Enumeration<URL> urls)
      {
         while (urls.hasMoreElements())
         {
            URL url = urls.nextElement();
            this.urls.add(url);
         }
      }
   }
}
