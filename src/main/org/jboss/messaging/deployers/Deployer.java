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

import org.jboss.logging.Logger;
import org.jboss.messaging.core.MessagingComponent;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

/**
 * abstract class that helps with deployment of messaging components.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public abstract class Deployer implements Deployable, MessagingComponent
{
   private static Logger log = Logger.getLogger(Deployer.class);
   protected static final String NAME_ATTR = "name";

   private HashMap<URL, HashMap<String, Node>> configuration = new HashMap<URL, HashMap<String, Node>>();

   /**
    * adds a URL to the already configured set of url's this deployer is handling
    * @param url The URL to add
    * @param name the name of the element
    * @param e .
    */
   public void addToConfiguration(URL url, String name, Node e)
   {
      if (configuration.get(url) == null)
      {
         configuration.put(url, new HashMap<String, Node>());
      }
      configuration.get(url).put(name, e);
   }

   /**
    * Redeploys a URL if changed
    *
    * @param url The resource to redeploy
    * @throws Exception .
    */
   public void redeploy(URL url) throws Exception
   {
      Element e = getRootElement(url);
      ArrayList<String> added = new ArrayList<String>();
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
            if (configuration.get(url) == null || (configuration.get(url) != null && configuration.get(url).get(name) == null))
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
      //now check for anything thathas been removed and undeploy
      if (configuration.get(url) != null)
      {
         Set<String> keys = configuration.get(url).keySet();
         ArrayList<String> removed = new ArrayList<String>();

         for (String key : keys)
         {
            if(!added.contains(key))
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

   protected Element getRootElement(URL url)
           throws Exception
   {
      return XMLUtil.urlToElement(url);
   }

   private boolean hasNodeChanged(URL url, Node child, String name)
   {
      String newTextContent = child.getTextContent();
      String origTextContent = configuration.get(url).get(name).getTextContent();
      return !newTextContent.equals(origTextContent);
   }

   /**
    * Undeploys a resource that has been removed
    * @param url The Resource that was deleted
    * @throws Exception .
    */
   public void undeploy(URL url) throws Exception
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
   public void deploy(URL url) throws Exception
   {
      Element e = getRootElement(url);
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
               log.error("key attribuet missing for configuration " + node);
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
   public void start() throws Exception
   {
      DeploymentManager.getInstance().registerDeployable(this);
   }

   //undeploy everything
   public void stop() throws Exception
   {
      Collection<HashMap<String, Node>> urls = configuration.values();
      for (HashMap<String, Node> hashMap : urls)
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
      DeploymentManager.getInstance().unregisterDeployable(this);  
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
   public abstract void deploy(Node node)
           throws Exception;

   /**
    * undeploys an element
    * @param node the element to undeploy
    * @throws Exception .
    */
   public abstract void undeploy(Node node)
           throws Exception;

}
