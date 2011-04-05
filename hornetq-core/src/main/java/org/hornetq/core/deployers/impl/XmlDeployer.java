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

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hornetq.core.deployers.Deployer;
import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQComponent;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public abstract class XmlDeployer implements Deployer, HornetQComponent
{
   private static Logger log = Logger.getLogger(XmlDeployer.class);

   protected static final String NAME_ATTR = "name";

   private final Map<URL, Map<String, Node>> configuration = new HashMap<URL, Map<String, Node>>();

   private final DeploymentManager deploymentManager;

   private boolean started;

   private String[] configFileNames;

   public XmlDeployer(final DeploymentManager deploymentManager)
   {
      this.deploymentManager = deploymentManager;
      configFileNames = getDefaultConfigFileNames();
   }

   /**
    * adds a URL to the already configured set of url's this deployer is handling
    * @param url The URL to add
    * @param name the name of the element
    * @param e .
    */
   public synchronized void addToConfiguration(final URL url, final String name, final Node e)
   {
      Map<String, Node> map = configuration.get(url);
      if (map == null)
      {
         map = new HashMap<String, Node>();
         configuration.put(url, map);
      }
      map.put(name, e);
   }

   /**
    * Redeploys a URL if changed
    *
    * @param url The resource to redeploy
    * @throws Exception .
    */
   public synchronized void redeploy(final URL url) throws Exception
   {
      Element e = getRootElement(url);
      List<String> added = new ArrayList<String>();
      // pull out the elements that need deploying
      String elements[] = getElementTagName();
      for (String element : elements)
      {
         NodeList children = e.getElementsByTagName(element);
         for (int i = 0; i < children.getLength(); i++)
         {
            Node node = children.item(i);
            String name = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
            added.add(name);
            // if this has never been deployed deploy
            Map<String, Node> map = configuration.get(url);
            if (map == null || map != null && map.get(name) == null)
            {
               deploy(node);
            }
            // or if it has changed redeploy
            else if (hasNodeChanged(url, node, name))
            {
               undeploy(node);
               deploy(node);
               addToConfiguration(url, name, node);
            }
         }
      }
      // now check for anything that has been removed and undeploy
      if (configuration.get(url) != null)
      {
         Set<String> keys = configuration.get(url).keySet();
         List<String> removed = new ArrayList<String>();

         for (String key : keys)
         {
            if (!added.contains(key))
            {
               undeploy(configuration.get(url).get(key));
               removed.add(key);
            }
         }
         for (String s : removed)
         {
            configuration.get(url).remove(s);
         }
      }
   }

   /**
    * Undeploys a resource that has been removed
    * @param url The Resource that was deleted
    * @throws Exception .
    */
   public synchronized void undeploy(final URL url) throws Exception
   {
      Set<String> keys = configuration.get(url).keySet();
      for (String key : keys)
      {
         undeploy(configuration.get(url).get(key));
      }
      configuration.remove(url);
   }

   /**
    * Deploy the URL for the first time
    *
    * @param url The resource todeploy
    * @throws Exception .
    */
   public synchronized void deploy(final URL url) throws Exception
   {
      Element e = getRootElement(url);

      validate(e);

      Map<String, Node> map = configuration.get(url);
      if (map == null)
      {
         map = new HashMap<String, Node>();
         configuration.put(url, map);
      }

      // find all thenodes to deploy
      String elements[] = getElementTagName();
      for (String element : elements)
      {
         NodeList children = e.getElementsByTagName(element);
         for (int i = 0; i < children.getLength(); i++)
         {
            Node node = children.item(i);
            Node keyNode = node.getAttributes().getNamedItem(getKeyAttribute());
            if (keyNode == null)
            {
               XmlDeployer.log.error("key attribute missing for configuration " + node);
               continue;
            }
            String name = keyNode.getNodeValue();
            try
            {
               deploy(node);
            }
            catch (Exception e1)
            {
               XmlDeployer.log.error(new StringBuilder("Unable to deploy node " + node + " " + name), e1);
               continue;
            }
            addToConfiguration(url, name, node);
         }
      }
   }

   /**
    * the key attribute for theelement, usually 'name' but can be overridden
    * @return the key attribute
    */
   public String getKeyAttribute()
   {
      return XmlDeployer.NAME_ATTR;
   }

   // register with the deploymenmt manager
   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      deploymentManager.registerDeployer(this);

      started = true;
   }

   // undeploy everything
   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      Collection<Map<String, Node>> urls = configuration.values();
      for (Map<String, Node> hashMap : urls)
      {
         for (Node node : hashMap.values())
         {
            try
            {
               undeploy(node);
            }
            catch (Exception e)
            {
               XmlDeployer.log.warn("problem undeploying " + node, e);
            }
         }
      }
      deploymentManager.unregisterDeployer(this);

      started = false;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   public String[] getConfigFileNames()
   {
      return configFileNames;
   }

   public void setConfigFileNames(final String[] configFileNames)
   {
      this.configFileNames = configFileNames;
   }

   /**
    * the names of the elements to deploy
    * @return the names of the elements todeploy
    */
   public abstract String[] getElementTagName();

   public abstract String[] getDefaultConfigFileNames();

   /**
    * deploy an element
    * @param node the element to deploy
    * @throws Exception .
    */
   public abstract void deploy(final Node node) throws Exception;

   /**
    * Validate the DOM 
    */
   public abstract void validate(final Node rootNode) throws Exception;

   /**
    * undeploys an element
    * @param node the element to undeploy
    * @throws Exception .
    */
   public abstract void undeploy(final Node node) throws Exception;

   protected Element getRootElement(final URL url) throws Exception
   {
      Reader reader = new InputStreamReader(url.openStream());
      String xml = org.hornetq.utils.XMLUtil.readerToString(reader);
      xml = org.hornetq.utils.XMLUtil.replaceSystemProps(xml);
      return org.hornetq.utils.XMLUtil.stringToElement(xml);
   }

   private boolean hasNodeChanged(final URL url, final Node child, final String name)
   {
      String newTextContent = child.getTextContent();
      String origTextContent = configuration.get(url).get(name).getTextContent();
      return !newTextContent.equals(origTextContent);
   }

}
