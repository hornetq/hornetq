/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Controller
{
   private static final Logger log = Logger.getLogger(Controller.class);   
   
   protected List tests;
   
   protected ResultPersistor persistor;
   
   public static void main(String[] args)
   {
      log.info("Controller starting");
      
      new Controller().run();
   }
   
   private Controller()
   {      
      tests = new ArrayList();
      
   }
   
   protected Object sendRequestToSlave(String slaveURL, ServerRequest request) throws Throwable
   {
      InvokerLocator locator = new InvokerLocator(slaveURL);
      Client client = new Client(locator, "perftest");
      Object res = client.invoke(request);
      return res;
   }
   
   protected Document getConfigDocument(String filename) throws Exception
   {
      InputStream is = null;
      try
      {
         File f = new File("controller.xml");   
         
         is = new FileInputStream(f);
         
         DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
   
         docBuilderFactory.setValidating(false);
   
         docBuilderFactory.setNamespaceAware(true);
   
         DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
         
         Document doc = docBuilder.parse(is);
         
         return doc;
      }
      finally
      {
         if (is != null)
         {
            try
            {
               is.close();
            }
            catch (IOException e)
            {
               log.error("Failed to close input stream", e);
            }
         }
      }
   }
   
   protected void init() throws Exception
   {
      persistor = new CSVResultPersistor("perf-results.csv");
      
      Document doc = getConfigDocument("controller.xml");
      
      Element root = doc.getDocumentElement();
                
      log.info("root element name is " + root.getNodeName());
      
      Element testsElement = MetadataUtils.getUniqueChild(root, "tests");             
      Iterator iter = MetadataUtils.getChildrenByTagName(testsElement, "test");
      
      while (iter.hasNext())
      {
         Element testElement = (Element)iter.next();
         
         Test test = new Test(testElement, persistor);
         
         tests.add(test); 
         
         log.info("Added test");
      } 
   }
   
   
   protected void run()
   {
      try
      {
        init();
        
        Iterator iter = tests.iterator();
        while (iter.hasNext())
        {
           Test test = (Test)iter.next();
           test.run();
        }
        
        log.info("Run all tests");
        
        persistor.close();
        
      }
      catch (Throwable e)
      {
         log.error("Failure in controller", e);
      }

   }
}
