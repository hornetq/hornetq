/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.util.ArrayList;
import java.util.Iterator;

import org.jboss.util.StringPropertyReplacer;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Mainly taken from org.jboss.metadata.MetaData
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:sebastien.alborini@m4x.org">Sebastien Alborini</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class MetadataUtils implements XMLLoadable
{
   /**
    * Returns an iterator over the children of the given element with
    * the given tag name.
    *
    * @param element    The parent element
    * @param tagName    The name of the desired child
    * @return           An interator of children or null if element is null.
    */
   public static Iterator getChildrenByTagName(Element element,
                                               String tagName)
   {
      if (element == null) return null;
      // getElementsByTagName gives the corresponding elements in the whole 
      // descendance. We want only children

      NodeList children = element.getChildNodes();
      ArrayList goodChildren = new ArrayList();
      for (int i=0; i<children.getLength(); i++) {
         Node currentChild = children.item(i);
         if (currentChild.getNodeType() == Node.ELEMENT_NODE && 
             ((Element)currentChild).getTagName().equals(tagName)) {
            goodChildren.add(currentChild);
         }
      }
      return goodChildren.iterator();
   }

   /**
    * Gets the child of the specified element having the specified unique
    * name.  If there are more than one children elements with the same name
    * and exception is thrown.
    *
    * @param element    The parent element
    * @param tagName    The name of the desired child
    * @return           The named child.
    *
    * @throws ConfigurationException   Child was not found or was not unique.
    */
   public static Element getUniqueChild(Element element, String tagName)
      throws ConfigurationException
   {
      Iterator goodChildren = getChildrenByTagName(element, tagName);

      if (goodChildren != null && goodChildren.hasNext()) {
         Element child = (Element)goodChildren.next();
         if (goodChildren.hasNext()) {
            throw new ConfigurationException
               ("expected only one " + tagName + " tag");
         }
         return child;
      } else {
         throw new ConfigurationException
            ("expected one " + tagName + " tag");
      }
   }

   /**
    * Gets the child of the specified element having the
    * specified name. If the child with this name doesn't exist
    * then null is returned instead.
    *
    * @param element the parent element
    * @param tagName the name of the desired child
    * @return either the named child or null
    */
   public static Element getOptionalChild(Element element, String tagName)
      throws ConfigurationException
   {
      return getOptionalChild(element, tagName, null);
   }

   /**
    * Gets the child of the specified element having the
    * specified name. If the child with this name doesn't exist
    * then the supplied default element is returned instead.
    *
    * @param element the parent element
    * @param tagName the name of the desired child
    * @param defaultElement the element to return if the child
    *                       doesn't exist
    * @return either the named child or the supplied default
    */
   public static Element getOptionalChild(Element element,
                                          String tagName,
                                          Element defaultElement)
      throws ConfigurationException
   {
      Iterator goodChildren = getChildrenByTagName(element, tagName);

      if (goodChildren != null && goodChildren.hasNext()) {
         Element child = (Element)goodChildren.next();
         if (goodChildren.hasNext()) {
            throw new ConfigurationException
               ("expected only one " + tagName + " tag");
         }
         return child;
      } else {
         return defaultElement;
      }
   }

   /**
    * Get an attribute value of the given element.
    *
    * @param element    The element to get the attribute value for.
    * @param attrName   The attribute name
    * @return           The attribute value or null.
    */
   public static String getElementAttribute(final Element element, final String attrName)
   {
      return getElementAttribute(element, attrName, true);
   }

   /**
    * Get an attribute value of the given element.
    *
    * @param element    The element to get the attribute value for.
    * @param attrName   The attribute name
    * @param replace    Whether to replace system properties
    * @return           The attribute value or null.
    */
   public static String getElementAttribute(final Element element, final String attrName, boolean replace)
   {
      if (element == null)
         return null;

      if (attrName == null || element.hasAttribute(attrName) == false)
         return null;

      String result = element.getAttribute(attrName);
      if (replace)
         return StringPropertyReplacer.replaceProperties(result.trim());
      else
         return result.trim();
   }

   /**
    * Get the content of the given element.
    *
    * @param element    The element to get the content for.
    * @return           The content of the element or null.
    */
   public static String getElementContent(final Element element)
   {
      return getElementContent(element, null);
   }

   /**
    * Get the content of the given element.
    *
    * @param element       The element to get the content for.
    * @param defaultStr    The default to return when there is no content.
    * @return              The content of the element or the default.
    */
   public static String getElementContent(Element element, String defaultStr)
   {
      return getElementContent(element, defaultStr, true);
   }

   /**
    * Get the content of the given element.
    *
    * @param element       The element to get the content for.
    * @param defaultStr    The default to return when there is no content.
    * @param replace       Whether to replace system properties
    * @return              The content of the element or the default.
    */
   public static String getElementContent(Element element, String defaultStr, boolean replace)
   {
      if (element == null)
         return defaultStr;

      NodeList children = element.getChildNodes();
      String result = "";
      for (int i = 0; i < children.getLength(); i++)
      {
         if (children.item(i).getNodeType() == Node.TEXT_NODE || 
             children.item(i).getNodeType() == Node.CDATA_SECTION_NODE)
         {
            result += children.item(i).getNodeValue();
         }
         else if( children.item(i).getNodeType() == Node.COMMENT_NODE )
         {
            // Ignore comment nodes
         }
         else
         {
            result += children.item(i).getFirstChild();
         }
      }
      if (replace)
         return StringPropertyReplacer.replaceProperties(result.trim());
      else
         return result.trim();
   }

   public static String getFirstElementContent(Element element, String defaultStr)
   {
      return getFirstElementContent(element, defaultStr, true);
   }

   public static String getFirstElementContent(Element element, String defaultStr, boolean replace)
   {
      if (element == null)
         return defaultStr;

      NodeList children = element.getChildNodes();
      String result = "";
      for (int i = 0; i < children.getLength(); i++)
      {
         if (children.item(i).getNodeType() == Node.TEXT_NODE ||
             children.item(i).getNodeType() == Node.CDATA_SECTION_NODE)
         {
            String val = children.item(i).getNodeValue();
            result += val;
         }
         else if( children.item(i).getNodeType() == Node.COMMENT_NODE )
         {
            // Ignore comment nodes
         }
         /*  For some reason, the original version gets the text of the first child
         else
         {
            result += children.item(i).getFirstChild();
         }
         */
      }
      if (replace)
         return StringPropertyReplacer.replaceProperties(result.trim());
      else
         return result.trim();
   }

   /**
    * Macro to get the content of a unique child element.
    *
    * @param element    The parent element.
    * @param tagName    The name of the desired child.
    * @return           The element content or null.
    */
   public static String getUniqueChildContent(Element element,
                                              String tagName)
      throws ConfigurationException
   {
      return getElementContent(getUniqueChild(element, tagName));
   }

   /**
    * Macro to get the content of an optional child element.
    * 
    * @param element    The parent element.
    * @param tagName    The name of the desired child.
    * @return           The element content or null.
    */
   public static String getOptionalChildContent(Element element,
                                                String tagName)
      throws ConfigurationException
   {
      return getElementContent(getOptionalChild(element, tagName));
   }

   /**
    * Macro to get the content of an optional child element with default value.
    *
    * @param element    The parent element.
    * @param tagName    The name of the desired child.
    * @return           The element content or null.
    */
   public static String getOptionalChildContent(Element element, String tagName, String defaultValue)
           throws ConfigurationException
   {
      return getElementContent(getOptionalChild(element, tagName), defaultValue);
   }

   public static boolean getOptionalChildBooleanContent(Element element, String name)
      throws ConfigurationException
   {
      Element child = getOptionalChild(element, name);
      if(child != null)
      {
         String value = getElementContent(child).toLowerCase();
         return value.equalsIgnoreCase("true") || value.equalsIgnoreCase("yes");
      }

      return false;
   }

   public static boolean getOptionalChildBooleanContent(Element element,
      String name, boolean defaultValue)
      throws ConfigurationException
   {
      Element child = getOptionalChild(element, name);
      boolean flag = defaultValue;
      if(child != null)
      {
         String value = getElementContent(child).toLowerCase();
         flag = value.equalsIgnoreCase("true") || value.equalsIgnoreCase("yes");
      }

      return flag;
   }
   
}
