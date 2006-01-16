/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.jboss;

import org.w3c.dom.Node;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.jboss.test.messaging.tools.xml.XMLUtil;

import javax.management.ObjectName;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

/**
 * The convenience object model of a JBoss <mbean> service descriptor configuration element.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class MBeanConfigurationElement
 {
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Element delegate;
   protected ObjectName on;
   protected String className;
   protected Map mbeanConfigAttributes;
   protected Map mbeanOptionalAttributeNames;

   // Constructors --------------------------------------------------

   public MBeanConfigurationElement(Node delegate) throws Exception
   {
      if (!delegate.getNodeName().equals("mbean"))
      {
         throw new IllegalStateException("This is NOT an mbean, it's an " +
                                         delegate.getNodeName() + "!");
      }

      this.delegate = (Element)delegate;

      NamedNodeMap attrs = delegate.getAttributes();

      Node mbeanNameAttr = attrs.getNamedItem("name");
      on = new ObjectName(mbeanNameAttr.getNodeValue());

      Node mbeanCodeAttr = attrs.getNamedItem("code");
      className = mbeanCodeAttr.getNodeValue();

      mbeanConfigAttributes = new HashMap();
      mbeanOptionalAttributeNames = new HashMap();

      if (delegate.hasChildNodes())
      {
         NodeList l = delegate.getChildNodes();
         for(int i = 0; i < l.getLength(); i ++)
         {
            Node mbeanConfigNode = l.item(i);
            String mbeanConfigNodeName = mbeanConfigNode.getNodeName();
            if ("attribute".equals(mbeanConfigNodeName))
            {
               attrs = mbeanConfigNode.getAttributes();
               String configAttribName = attrs.getNamedItem("name").getNodeValue();
               String configAttribValue = null;
               Node n = attrs.getNamedItem("value");
               if (n != null)
               {
                  configAttribValue = n.getNodeValue();
               }
               else
               {
                  configAttribValue = XMLUtil.getTextContent(mbeanConfigNode);
               }
               mbeanConfigAttributes.put(configAttribName, configAttribValue);
            }
            else if ("depends".equals(mbeanConfigNodeName))
            {
               attrs = mbeanConfigNode.getAttributes();
               Node optionalAttributeNode = attrs.getNamedItem("optional-attribute-name");
               if (optionalAttributeNode != null)
               {
                  String optionalAttributeName = optionalAttributeNode.getNodeValue();
                  String optionalAttributeValue = XMLUtil.getTextContent(mbeanConfigNode);
                  mbeanOptionalAttributeNames.put(optionalAttributeName, optionalAttributeValue);
               }
            }
         }
      }
   }

   // Public --------------------------------------------------------

   public Element getDelegate()
   {
      return delegate;
   }

   public ObjectName getObjectName()
   {
      return on;
   }

   /**
    * Returns the fully qualified name of the class that implements the service.
    */
   public String getMBeanClassName()
   {
      return className;
   }

   /**
    * @return Set<String>
    */
   public Set attributeNames()
   {
      return mbeanConfigAttributes.keySet();
   }

   public String getAttributeValue(String name)
   {
      return (String)mbeanConfigAttributes.get(name);
   }

   /**
    * @return Set<String>
    */
   public Set dependencyOptionalAttributeNames()
   {
      return mbeanOptionalAttributeNames.keySet();
   }

   public String getDependencyOptionalAttributeValue(String optionalAttributeName)
   {
      return (String)mbeanOptionalAttributeNames.get(optionalAttributeName);
   }

   public String toString()
   {
      return getMBeanClassName() + "[" + getObjectName() + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
