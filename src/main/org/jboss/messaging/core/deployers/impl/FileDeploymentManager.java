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
package org.jboss.messaging.core.deployers.impl;

import org.jboss.messaging.core.deployers.Deployer;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class FileDeploymentManager implements Runnable, DeploymentManager
{
   private static final Logger log = Logger.getLogger(FileDeploymentManager.class);
   //these are the list of deployers, typically destination and connection factory.
   private static List<Deployer> deployers = new ArrayList<Deployer>();
   //any config files deployed and the time they were deployed
   private static Map<URL, Long> deployed = new HashMap<URL, Long>();
   // the list of URL's to deploy
   private static List<URL> toDeploy = new ArrayList<URL>();
   //the list of URL's to undeploy if removed
   private static List<URL> toUndeploy = new ArrayList<URL>();
   //the list of URL's to redeploy if changed
   private static List<URL> toRedeploy = new ArrayList<URL>();

   private static ScheduledExecutorService scheduler;

   public synchronized void start() throws Exception
   {
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

      scheduler.scheduleAtFixedRate(this, 10, 5, TimeUnit.SECONDS);
   }

   public synchronized void stop()
   {
      deployers.clear();
      if (scheduler != null)
      {
         scheduler.shutdown();
         scheduler = null;
      }
   }

   /**
    * registers a Deployer object which will handle the deployment of URL's
    *
    * @param Deployer The Deployer object
    * @throws Exception .
    */
   public synchronized void registerDeployer(final Deployer Deployer) throws Exception
   {
      deployers.add(Deployer);
      Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(Deployer.getConfigFileName());
      while (urls.hasMoreElements())
      {
         URL url = urls.nextElement();
         if (!deployed.keySet().contains(url))
         {
            deployed.put(url, new File(url.getFile()).lastModified());
         }
         try
         {
            log.info(new StringBuilder("Deploying ").append(Deployer).append(" with url").append(url));
            Deployer.deploy(url);

         }
         catch (Exception e)
         {
            log.error(new StringBuilder("Error deploying ").append(url), e);
         }
      }
   }

   public synchronized void unregisterDeployer(final Deployer Deployer)
   {
      deployers.remove(Deployer);
      if (deployers.size() == 0)
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
   public synchronized void run()
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

   /**
    * will return any resources available
    *
    * @return a set of configurationUrls
    * @throws java.io.IOException .
    */
   private Collection<ConfigurationURL> getConfigurations() throws IOException
   {
      Map<String, ConfigurationURL> configurations = new HashMap<String, ConfigurationURL>();
      for (Deployer deployer : deployers)
      {
         Enumeration<URL> urls = Thread.currentThread().getContextClassLoader().getResources(deployer.getConfigFileName());

         if (!configurations.keySet().contains(deployer.getConfigFileName()))
         {
            ConfigurationURL conf = new ConfigurationURL(urls, deployer.getConfigFileName());
            configurations.put(deployer.getConfigFileName(), conf);
         }
         else
         {
            configurations.get(deployer.getConfigFileName()).add(urls);
         }
      }
      return configurations.values();
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
    * undeploys a url, delegates to appropiate registered Deployers
    *
    * @param url the url to undeploy
    */
   private void undeploy(final URL url)
   {
      deployed.remove(url);

      for (Deployer Deployer : deployers)
      {
         try
         {
            log.info(new StringBuilder("Undeploying ").append(Deployer).append(" with url").append(url));
            Deployer.undeploy(url);
         }
         catch (Exception e)
         {
            log.error(new StringBuilder("Error undeploying ").append(url), e);
         }
      }
   }

   /**
    * redeploys a url, delegates to appropiate registered Deployers
    *
    * @param url the url to redeploy
    */
   private void redeploy(final URL url)
   {
      deployed.put(url, new File(url.getFile()).lastModified());
      for (Deployer Deployer : deployers)
      {
         try
         {
            log.info(new StringBuilder("Redeploying ").append(Deployer).append(" with url").append(url));
            Deployer.redeploy(url);
         }
         catch (Exception e)
         {
            log.error(new StringBuilder("Error redeploying ").append(url), e);
         }
      }
   }

   /**
    * deploys a url, delegates to appropiate registered Deployers
    *
    * @param url the url to deploy
    * @throws Exception .
    */
   private void deploy(final URL url) throws Exception
   {
      deployed.put(url, new File(url.getFile()).lastModified());
      for (Deployer Deployer : deployers)
      {
         try
         {
            log.info(new StringBuilder("Deploying ").append(Deployer).append(" with url").append(url));
            Deployer.deploy(url);
         }
         catch (Exception e)
         {
            log.error(new StringBuilder("Error deploying ").append(url), e);
         }
      }
   }

   private static class ConfigurationURL
   {
      private List<URL> urls = new ArrayList<URL>();
      private String configFileName;

      public ConfigurationURL(final Enumeration<URL> urls, final String configFileName)
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

      public void add(final Enumeration<URL> urls)
      {
         while (urls.hasMoreElements())
         {
            URL url = urls.nextElement();
            this.urls.add(url);
         }
      }
   }
}

