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

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.deployers.Deployer;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessagingComponent;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public abstract class XmlDeployer implements Deployer, MessagingComponent
{
   private static Logger log = Logger.getLogger(XmlDeployer.class);
   
   protected static final String NAME_ATTR = "name";

   private final Map<URL, Map<String, Node>> configuration = new HashMap<URL, Map<String, Node>>();

   private final DeploymentManager deploymentManager;
   
   private boolean started;

   public XmlDeployer(final DeploymentManager deploymentManager)
   {
      this.deploymentManager = deploymentManager;
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
      //pull out the elements that need deploying
      String elements[] = getElementTagName();
      for (String element : elements)
      {
         NodeList children = e.getElementsByTagName(element);
         for (int i = 0; i < children.getLength(); i++)
         {
            Node node = children.item(i);
            String name = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
            added.add(name);
            //if this has never been deployed deploy
            Map<String, Node> map = configuration.get(url); 
            if (map == null || (map != null && map.get(name) == null))
            {
               log.info(new StringBuilder(name).append(" doesn't exist deploying"));
               deploy(node);
            }
            //or if it has changed redeploy
            else if (hasNodeChanged(url, node, name))
            {
               log.info(new StringBuilder(name).append(" has changed redeploying"));
               undeploy(node);
               deploy(node);
               addToConfiguration(url, name, node);
            }
         }
      }
      //now check for anything that has been removed and undeploy
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
      
      //find all thenodes to deploy
      String elements[] = getElementTagName();
      for (String element : elements)
      {
         NodeList children = e.getElementsByTagName(element);
         for (int i = 0; i < children.getLength(); i++)
         {
            Node node = children.item(i);
            Node keyNode = node.getAttributes().getNamedItem(getKeyAttribute());
            if(keyNode == null)
            {
               log.error("key attribute missing for configuration " + node);
               continue;
            }
            String name = keyNode.getNodeValue();
            log.info(new StringBuilder("deploying ").append(name));
            try
            {
               deploy(node);
            }
            catch (Exception e1)
            {
               log.error(new StringBuilder("Unable to deploy node ").append(node), e1);
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
      return NAME_ATTR;
   }

   //register with the deploymenmt manager
   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }
      
      deploymentManager.registerDeployer(this);
      
      started = true;
   }

   //undeploy everything
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
               log.warn("problem undeploying " + node, e);
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

   /**
    * the names of the elements to deploy
    * @return the names of the elements todeploy
    */
   public abstract String[] getElementTagName();


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
   public abstract void undeploy(final Node node)
           throws Exception;

   protected Element getRootElement(final URL url)
           throws Exception
   {
      Reader reader = new InputStreamReader(url.openStream());
      String xml = XMLUtil.readerToString(reader);
      xml = XMLUtil.replaceSystemProps(xml);
      return XMLUtil.stringToElement(xml);
   }

   private boolean hasNodeChanged(final URL url, final Node child, final String name)
   {
      String newTextContent = child.getTextContent();
      String origTextContent = configuration.get(url).get(name).getTextContent();
      return !newTextContent.equals(origTextContent);
   }

}
