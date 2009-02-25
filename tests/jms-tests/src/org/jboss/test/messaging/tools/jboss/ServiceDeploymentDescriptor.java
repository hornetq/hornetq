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
package org.jboss.test.messaging.tools.jboss;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.management.ObjectName;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.utils.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * The convenience object model of a JBoss service deployment descriptor (<server>).
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServiceDeploymentDescriptor
 {
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServiceDeploymentDescriptor.class);

   // Static ---------------------------------------------------------------------------------------
   
   // Attributes -----------------------------------------------------------------------------------

   // List<Element> contains the top level elements from under <server>
   private List elements;

   // Constructors ---------------------------------------------------------------------------------

   public ServiceDeploymentDescriptor(String config) throws Exception
   {
      this(org.jboss.messaging.utils.XMLUtil.stringToElement(config));
   }

   public ServiceDeploymentDescriptor(URL descriptorURL) throws Exception
   {
      this(XMLUtil.urlToElement(descriptorURL));
   }

   public ServiceDeploymentDescriptor(Element service) throws Exception
   {
      elements = new ArrayList();
      NodeList l = service.getChildNodes();
      for(int i = 0; i < l.getLength(); i++)
      {
         elements.add(l.item(i));
      }
   }

   // Public ---------------------------------------------------------------------------------------

   /**
    * Scans the list of ObjectNames present in this service configuration and returns a list
    * of MBeanConfigurationElements that match the criteria or an empty list.
    *
    * @return List<MBeanConfigurationElement>
    */
   public List query(String objectNameKey, String objectNameValue)
   {
      List result = null;
      for(Iterator i = elements.iterator(); i.hasNext(); )
      {
         Node n = (Node)i.next();
         if (n.getNodeName().equals("mbean"))
         {
            try
            {
               NamedNodeMap attrs = n.getAttributes();
               Node name = attrs.getNamedItem("name");
               ObjectName on = new ObjectName(name.getNodeValue());
               String s = on.getKeyProperty(objectNameKey);
               if (objectNameValue.equals(s))
               {
                  if (result == null)
                  {
                     result = new ArrayList();
                  }
                  result.add(new MBeanConfigurationElement(n));
               }
            }
            catch(Exception e)
            {
               log.error("failed to create MBeanConfigurationElement", e);
            }
         }
      }

      if (result == null)
      {
         return Collections.EMPTY_LIST;
      }

      return result;
   }

   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------
   
   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
