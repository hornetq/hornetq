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

import org.w3c.dom.Element;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SlaveMetadata extends XMLLoadableSupport implements Serializable
{
   private static final long serialVersionUID = 4340448965188777968L;

   protected String slaveLocator;
   
   protected List jobs = new ArrayList();
   
   public String getSlaveLocator()
   {
      return slaveLocator;
   }
   
   public List getJobs()
   {
      return Collections.unmodifiableList(jobs);
   }
   
   public SlaveMetadata(Element element)
      throws DeploymentException
   {
      importXML(element);
   }
   
   public void importXML(Element element) throws DeploymentException
   {
      slaveLocator = getUniqueChildContent(element, "slave-locator");
      Iterator iter = getChildrenByTagName(element, "sender-job");
      
      while (iter.hasNext())
      {
         Element el = (Element)iter.next();

         SenderJob job = new SenderJob(el);
         jobs.add(job);
         
      }
      
      iter = getChildrenByTagName(element, "receiver-job");
      
      while (iter.hasNext())
      {
         Element el = (Element)iter.next();
         
         ReceiverJob job = new ReceiverJob(el);

         jobs.add(job);
         
      }
   }
}
