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
package org.jboss.jms.client.container;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.jboss.aop.AspectManager;
import org.jboss.aop.AspectXmlLoader;
import org.w3c.dom.Document;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


/**
 * This class deploys the client side AOP config from a byte[] representation of the 
 * client aop config file.
 * This allows the config to be kep on the server but the advising to be done on the client.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class JmsClientAspectXMLLoader extends AspectXmlLoader
{
   public JmsClientAspectXMLLoader()
   {
      super();
      this.setManager(AspectManager.instance());
   }
   
   /*
    * Deploy aop config from byte[]
    */
   public void deployXML(byte[] config) throws Exception
   {
      InputStream is = null;
      
      try
      {
         is = new ByteArrayInputStream(config);      
      
         DocumentBuilderFactory docBuilderFactory = null;
         
         docBuilderFactory = DocumentBuilderFactory.newInstance();
         
         docBuilderFactory.setValidating(false);
         
         InputSource source = new InputSource(is);
         
         URL url = AspectXmlLoader.class.getResource("/jboss-aop_1_0.dtd");
         
         source.setSystemId(url.toString());
         
         DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
         
         docBuilder.setEntityResolver(new Resolver());
         
         Document doc = docBuilder.parse(source);
         
         this.deployXML(doc, null);              
      }
      finally
      {
         if (is != null)
         {
            is.close();
         }
      }
   }
   
   /* From AspectXMLLoader.Resolver */
   private static class Resolver implements EntityResolver
   {
      public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException
      {
         if (systemId.endsWith("jboss-aop_1_0.dtd"))
         {
            try
            {
               URL url = AspectXmlLoader.class.getResource("/jboss-aop_1_0.dtd");
               InputStream is = url.openStream();
               return new InputSource(is);
            }
            catch (IOException e)
            {
               e.printStackTrace();
               return null;
            }
         }
         return null;
      }
   }
}
