/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.jboss.logging.Logger;
import org.w3c.dom.Element;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Configuration implements XMLLoadable, Serializable
{
   private static final long serialVersionUID = -4447924453439700666L;

   private static final Logger log = Logger.getLogger(Configuration.class);   
   
   protected List slaves = new ArrayList();
   
   protected String name;
   
   public String getName()
   {
      return name;
   }
   
   public void importXML(Element e) throws DeploymentException
   {
      log.info("Loading configuration");
      
      name = MetadataUtils.getElementAttribute(e, "name");
      
      log.info("Name is " + name);
      
      Iterator iter = MetadataUtils.getChildrenByTagName(e, "slave");
      
      while (iter.hasNext())
      {
         Element element = (Element)iter.next();
         SlaveMetadata slave = new SlaveMetadata(element);
         slaves.add(slave);
      }
   }
   
   public Configuration(Element element) throws DeploymentException
   {
      importXML(element);
   }
   
   public List getSlaves()
   {
      return Collections.unmodifiableList(slaves);
   }
      
}
