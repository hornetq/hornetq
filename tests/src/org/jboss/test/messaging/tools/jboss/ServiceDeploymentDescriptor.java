/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.jboss;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.NamedNodeMap;
import org.jboss.jms.util.XMLUtil;
import org.jboss.logging.Logger;
import org.jboss.jms.util.XMLUtil;

import javax.management.ObjectName;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collections;

/**
 * The convenience object model of a JBoss service deployment descriptor (<server>).
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServiceDeploymentDescriptor
 {
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServiceDeploymentDescriptor.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // List<Element> contains the top level elements from under <server>
   private List elements;

   // Constructors --------------------------------------------------

   public ServiceDeploymentDescriptor(String config) throws Exception
   {
      this(XMLUtil.stringToElement(config));
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

   // Public --------------------------------------------------------

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

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
