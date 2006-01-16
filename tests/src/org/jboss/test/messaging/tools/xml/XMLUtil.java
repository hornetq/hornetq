/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.xml;

import org.w3c.dom.Element;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.jboss.logging.Logger;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.io.StringReader;
import java.io.Reader;
import java.io.InputStreamReader;
import java.net.URL;
import java.lang.reflect.Method;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class XMLUtil
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(XMLUtil.class);

   // Static --------------------------------------------------------

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
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder parser = factory.newDocumentBuilder();
      Document doc = parser.parse(new InputSource(r));
      return doc.getDocumentElement();
   }

   public static String elementToString(Node n) throws Exception
   {

      String name = n.getNodeName();
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
            sb.append(' ').append(attr.getNodeName() + "=\"" + attr.getNodeValue() + "\"");
         }
      }

      NodeList children = n.getChildNodes();

      if (children.getLength() == 0)
      {
         sb.append("/>").append('\n');
      }
      else
      {
         sb.append('>').append('\n');
         for(int i = 0; i < children.getLength(); i++)
         {
            sb.append(elementToString(children.item(i)));

         }
         sb.append("</").append(name).append('>');
      }
      return sb.toString();
   }


   private static final Object[] EMPTY_ARRAY = new Object[0];

   /**
    * This metod is here because Node.getTextContent() is not available in JDK 1.4 and I would like
    * to have an uniform access to this functionality.
    */
   public static String getTextContent(Node n)
   {
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

      // JDK 1.4

      String s = n.toString();
      int i = s.indexOf('>');
      int i2 = s.indexOf("</");
      if (i == -1 || i2 == -1)
      {
         log.error("Invalid string expression: " + s);
         return null;
      }
      return s.substring(i + 1, i2);

   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
