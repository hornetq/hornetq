/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.util;

import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.jboss.logging.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class XMLUtil
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(XMLUtil.class);

   // Static ---------------------------------------------------------------------------------------

   public static Element stringToElement(String s) throws Exception
   {
      return readerToElement(new StringReader(s));
   }

   public static Element urlToElement(URL url) throws Exception
   {
      return readerToElement(new InputStreamReader(url.openStream()));
   }

   public static Element readerToElement(Reader r) throws Exception
   {
      //Read into string
      StringBuffer buff = new StringBuffer();
      int c;
      while ((c = r.read()) != -1)            
      {
         buff.append((char)c);
      }
      
      //Quick hardcoded replace, FIXME this is a kludge - use regexp to match properly
      String s = buff.toString();
      s = doReplace(s, "jboss.messaging.groupname", "MessagingPostOffice");
      s = doReplace(s, "jboss.messaging.datachanneludpaddress", "228.6.6.6");
      s = doReplace(s, "jboss.messaging.controlchanneludpaddress", "228.7.7.7");
      s = doReplace(s, "jboss.messaging.datachanneludpport", "45567");
      s = doReplace(s, "jboss.messaging.controlchanneludpport", "45568");
      s = doReplace(s, "jboss.messaging.ipttl", "2");
      s = doReplace(s, "jboss.messaging.ipttl", "8");
      
      StringReader sreader = new StringReader(s);
       
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder parser = factory.newDocumentBuilder();
      Document doc = parser.parse(new InputSource(sreader));
      return doc.getDocumentElement();
   }
   
   public static String elementToString(Node n)
   {

      String name = n.getNodeName();

      short type = n.getNodeType();

      if (Node.CDATA_SECTION_NODE == type)
      {
         return "<![CDATA[" + n.getNodeValue() + "]]>";
      }

      if (name.startsWith("#"))
      {
         return "";
      }

      StringBuffer sb = new StringBuffer();
      sb.append('<').append(name);

      NamedNodeMap attrs = n.getAttributes();
      if (attrs != null)
      {
         for(int i = 0; i < attrs.getLength(); i++)
         {
            Node attr = attrs.item(i);
            sb.append(' ').
               append(attr.getNodeName()).
               append("=\"").
               append(attr.getNodeValue()).
               append("\"");
         }
      }

      String textContent = null;
      NodeList children = n.getChildNodes();

      if (children.getLength() == 0)
      {
         if ((textContent = XMLUtil.getTextContent(n)) != null && !"".equals(textContent))
         {
            sb.append(textContent).append("</").append(name).append('>');;
         }
         else
         {
            sb.append("/>").append('\n');
         }
      }
      else
      {
         sb.append('>').append('\n');
         boolean hasValidChildren = false;
         for(int i = 0; i < children.getLength(); i++)
         {
            String childToString = elementToString(children.item(i));
            if (!"".equals(childToString))
            {
               sb.append(childToString);
               hasValidChildren = true;
            }
         }

         if (!hasValidChildren && ((textContent = XMLUtil.getTextContent(n)) != null))
         {
            sb.append(textContent);
         }

         sb.append("</").append(name).append('>');
      }

      return sb.toString();
   }

   private static final Object[] EMPTY_ARRAY = new Object[0];

   /**
    * This metod is here because Node.getTextContent() is not available in JDK 1.4 and I would like
    * to have an uniform access to this functionality.
    *
    * Note: if the content is another element or set of elements, it returns a string representation
    *       of the hierarchy.
    *
    * TODO implementation of this method is a hack. Implement it properly.
    */
   public static String getTextContent(Node n)
   {
      if (n.hasChildNodes())
      {
         StringBuffer sb = new StringBuffer();
         NodeList nl = n.getChildNodes();
         for(int i = 0; i < nl.getLength(); i++)
         {
            sb.append(XMLUtil.elementToString(nl.item(i)));
            if (i < nl.getLength() - 1)
            {
               sb.append('\n');
            }
         }

         String s = sb.toString();
         if (s.length() != 0)
         {
            return s;
         }
      }

      Method[] methods = Node.class.getMethods();

      for(int i = 0; i < methods.length; i++)
      {
         if("getTextContent".equals(methods[i].getName()))
         {
            Method getTextContext = methods[i];
            try
            {
               return (String)getTextContext.invoke(n, EMPTY_ARRAY);
            }
            catch(Exception e)
            {
               log.error("Failed to invoke getTextContent() on node " + n, e);
               return null;
            }
         }
      }

      String textContent = null;

      if (n.hasChildNodes())
      {
         NodeList nl = n.getChildNodes();
         for(int i = 0; i < nl.getLength(); i++)
         {
            Node c = nl.item(i);
            if (c.getNodeType() == Node.TEXT_NODE)
            {
               textContent = n.getNodeValue();
               if (textContent == null)
               {
                  // TODO This is a hack. Get rid of it and implement this properly
                  String s = c.toString();
                  int idx = s.indexOf("#text:");
                  if (idx != -1)
                  {
                     textContent = s.substring(idx + 6).trim();
                     if (textContent.endsWith("]"))
                     {
                        textContent = textContent.substring(0, textContent.length() - 1);
                     }
                  }
               }
               if (textContent == null)
               {
                 break;
               }
            }
         }

         // TODO This is a hack. Get rid of it and implement this properly
         String s = n.toString();
         int i = s.indexOf('>');
         int i2 = s.indexOf("</");
         if (i != -1 && i2 != -1)
         {
            textContent = s.substring(i + 1, i2);
         }
      }

      return textContent;
   }

   public static void assertEquivalent(Node node, Node node2)
   {
      if (node == null)
      {
         throw new IllegalArgumentException("the first node to be compared is null");
      }

      if (node2 == null)
      {
         throw new IllegalArgumentException("the second node to be compared is null");
      }

      if (!node.getNodeName().equals(node2.getNodeName()))
      {
         throw new IllegalArgumentException("nodes have different node names");
      }

      int attrCount = 0;
      NamedNodeMap attrs = node.getAttributes();
      if (attrs != null)
      {
         attrCount = attrs.getLength();
      }

      int attrCount2 = 0;
      NamedNodeMap attrs2 = node2.getAttributes();
      if (attrs2 != null)
      {
         attrCount2 = attrs2.getLength();
      }

      if (attrCount != attrCount2)
      {
         throw new IllegalArgumentException("nodes hava a different number of attributes");
      }

      outer: for(int i = 0; i < attrCount; i++)
      {
         Node n = attrs.item(i);
         String name = n.getNodeName();
         String value = n.getNodeValue();

         for(int j = 0; j < attrCount; j++)
         {
            Node n2 = attrs2.item(j);
            String name2 = n2.getNodeName();
            String value2 = n2.getNodeValue();

            if (name.equals(name2) && value.equals(value2))
            {
               continue outer;
            }
         }
         throw new IllegalArgumentException("attribute " + name + "=" + value + " doesn't match");
      }

      boolean hasChildren = node.hasChildNodes();

      if (hasChildren != node2.hasChildNodes())
      {
         throw new IllegalArgumentException("one node has children and the other doesn't");
      }

      if (hasChildren)
      {
         NodeList nl = node.getChildNodes();
         NodeList nl2 = node2.getChildNodes();

         short[] toFilter = new short[] {Node.TEXT_NODE, Node.ATTRIBUTE_NODE, Node.COMMENT_NODE};
         List nodes = filter(nl, toFilter);
         List nodes2 = filter(nl2, toFilter);

         int length = nodes.size();

         if (length != nodes2.size())
         {
            throw new IllegalArgumentException("nodes hava a different number of children");
         }

         for(int i = 0; i < length; i++)
         {
            Node n = (Node)nodes.get(i);
            Node n2 = (Node)nodes2.get(i);
            assertEquivalent(n, n2);
         }
      }
   }

   public static String stripCDATA(String s)
   {
      s = s.trim();
      if (s.startsWith("<![CDATA["))
      {
         s = s.substring(9);
         int i = s.indexOf("]]>");
         if (i == -1)
         {
            throw new IllegalStateException(
               "argument starts with <![CDATA[ but cannot find pairing ]]>");
         }
         s = s.substring(0, i);
      }
      return s;
   }

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------
   
   // Private --------------------------------------------------------------------------------------

   private static List filter(NodeList nl, short[] typesToFilter)
   {
      List nodes = new ArrayList();

      outer: for(int i = 0; i < nl.getLength(); i++)
      {
         Node n = nl.item(i);
         short type = n.getNodeType();
         for(int j = 0; j < typesToFilter.length; j++)
         {
            if (typesToFilter[j] == type)
            {
               continue outer;
            }
         }
         nodes.add(n);
      }
      return nodes;
   }
   
   //Quick dirty replace - use a reg exp to use true default value
   private static String doReplace(String s, String propertyName, String defaultValue)
   {
      String sysProp = System.getProperty(propertyName);
      
      s = s.replace("${" + propertyName + ":" + defaultValue + "}", sysProp == null ? defaultValue : sysProp);
      
      return s;
   }


   // Inner classes --------------------------------------------------------------------------------
}
