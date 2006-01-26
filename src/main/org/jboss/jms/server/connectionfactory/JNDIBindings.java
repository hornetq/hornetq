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
package org.jboss.jms.server.connectionfactory;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.jboss.jms.util.XMLException;
import org.jboss.jms.util.XMLUtil;

import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JNDIBindings
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private Element delegate;
   private List names;

   // Constructors --------------------------------------------------

   public JNDIBindings(Element delegate) throws XMLException
   {
      parse(delegate);
      this.delegate = delegate;
   }

   // Public --------------------------------------------------------

   public Element getDelegate()
   {
      return delegate;
   }

   /**
    * @return List<String>
    */
   public List getNames()
   {
      return names;
   }

   public String toString()
   {
      if (names == null)
      {
         return "";
      }

      StringBuffer sb = new StringBuffer();

      for(Iterator i = names.iterator(); i.hasNext(); )
      {
         sb.append(i.next());
         if (i.hasNext())
         {
            sb.append(',');
         }
      }
      return sb.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void parse(Element e) throws XMLException
   {
      if (!"bindings".equals(e.getNodeName()))
      {
         throw new XMLException("The element is not a <bindings> element");
      }

      if (!e.hasChildNodes())
      {
         names = Collections.EMPTY_LIST;
         return;
      }

      NodeList nl= e.getChildNodes();
      for(int i = 0; i < nl.getLength(); i++)
      {
         Node n = nl.item(i);
         if ("binding".equals(n.getNodeName()))
         {
            String text = XMLUtil.getTextContent(n).trim();
            if (names == null)
            {
               names = new ArrayList();
            }
            names.add(text);
         }
      }

      if (names == null)
      {
         names = Collections.EMPTY_LIST;
      }
   }

   // Inner classes -------------------------------------------------
}
