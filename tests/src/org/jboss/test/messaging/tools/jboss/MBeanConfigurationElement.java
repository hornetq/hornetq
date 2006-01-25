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
import org.jboss.jms.util.XMLUtil;

import javax.management.ObjectName;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

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

   public static Class stringToClass(String type) throws Exception
   {
      // TODO - very quick and very dirty implementation
      if ("java.lang.String".equals(type))
      {
         return String.class;
      }
      else if ("java.lang.Integer".equals(type))
      {
         return Integer.class;
      }
      else if ("int".equals(type))
      {
         return Integer.TYPE;
      }
      else
      {
         throw new Exception("Don't know to convert " + type + " to Class!");
      }
   }

   // Attributes ----------------------------------------------------

   protected Element delegate;
   protected ObjectName on;
   protected String className;
   protected String xmbeandd;
   protected Map mbeanConfigAttributes;
   protected Map mbeanOptionalAttributeNames;
   protected List constructors;



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

      Node mbeanXMBeanDD = attrs.getNamedItem("xmbean-dd");
      if (mbeanXMBeanDD != null)
      {
         xmbeandd = mbeanXMBeanDD.getNodeValue();
      }

      mbeanConfigAttributes = new HashMap();
      mbeanOptionalAttributeNames = new HashMap();
      constructors = new ArrayList();

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
            else if ("constructor".equals(mbeanConfigNodeName))
            {
               ConstructorElement c = new ConstructorElement(mbeanConfigNode);
               constructors.add(c);

               if (mbeanConfigNode.hasChildNodes())
               {
                  NodeList nl = mbeanConfigNode.getChildNodes();
                  for(int j = 0; j < nl.getLength(); j++)
                  {
                     Node n = nl.item(j);
                     String name = n.getNodeName();
                     if ("arg".equals(name))
                     {
                        NamedNodeMap at = n.getAttributes();
                        Node attributeNode = at.getNamedItem("type");
                        Class type = stringToClass(attributeNode.getNodeValue());
                        attributeNode = at.getNamedItem("value");
                        String value = attributeNode.getNodeValue();
                        c.addArgument(type, value, attributeNode);
                     }
                  }
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

   public void setAttribute(String name, String value)
   {
      mbeanConfigAttributes.put(name, value);
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

   public Class getConstructorArgumentType(int constructorIndex, int paramIndex)
   {
      return ((ConstructorElement)constructors.get(constructorIndex)).getArgType(paramIndex);
   }

   public String getConstructorArgumentValue(int constructorIndex, int paramIndex)
   {
      return ((ConstructorElement)constructors.get(constructorIndex)).getArgValue(paramIndex);
   }

   public void setConstructorArgumentValue(int constructorIndex, int paramIndex, String value)
      throws Exception
   {
      ConstructorElement c = ((ConstructorElement)constructors.get(constructorIndex));
      c.setArgValue(paramIndex, value);
   }


   public String toString()
   {
      return getMBeanClassName() + "[" + getObjectName() + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class ConstructorElement
   {
      // List<Class>
      private List argTypes = new ArrayList();
      // List<String>
      private List argValues = new ArrayList();
      // List<Node>
      private List argNodes = new ArrayList();

      protected Node node;

      public ConstructorElement(Node node)
      {
         this.node = node;
      }

      public void addArgument(Class type, String value, Node node)
      {
         argTypes.add(type);
         argValues.add(value);
         argNodes.add(node);
      }

      public Class getArgType(int idx)
      {
         return (Class)argTypes.get(idx);
      }

      public String getArgValue(int idx)
      {
         return (String)argValues.get(idx);
      }

      public void setArgValue(int idx, String value)
      {
         Node n = (Node)argNodes.get(idx);
         n.setNodeValue(value);
         argValues.set(idx, value);
      }
   }
}
