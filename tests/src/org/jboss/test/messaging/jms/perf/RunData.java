/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.io.Serializable;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RunData implements Serializable, ResultSource
{
   private static final long serialVersionUID = -8826202501525635283L;

   public String jobName;
   
   public double currentTP;   
   
   public void getResults(ResultPersistor persistor)
   {
      persistor.addValue("tp", currentTP);
   }
}
