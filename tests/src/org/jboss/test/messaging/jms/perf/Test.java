/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import org.jboss.logging.Logger;
import org.w3c.dom.Element;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Test extends XMLLoadableSupport
{
   private static final Logger log = Logger.getLogger(Test.class);   
   
   protected String name;
   
   protected String configName;
   
   protected long duration;
   
   public String getName()
   {
      return name;
   }
   
   public String getConfigName()
   {
      return configName;
   }
   
   public long getDuration()
   {
      return duration;
   }
   
   public Test(Element el) throws DeploymentException
   {
      importXML(el);
   }
   
   public void importXML(Element el) throws DeploymentException
   {
      log.info("Loading test:" + el.getNodeName());
      
      name = XMLLoadableSupport.getElementAttribute(el, "name");
      
      //name = XMLLoadableSupport.getElementAttribute(el, name);
      
      log.info("Test Name is " + name);
      
      configName = getUniqueChildContent(el, "config"); 
      
      duration = Long.parseLong(getUniqueChildContent(el, "duration"));
      
      log.info("config is" + configName);

   }
}
