/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import javax.jms.JMSException;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface MetaDataRepository
{

   public Object getMetaData(Object attr) throws JMSException;

   /**
    * Adds meta data as TRANSIENT.
    */
   public void addMetaData(Object attr, Object metaDataValue) throws JMSException;

   public Object removeMetaData(Object attr) throws JMSException;
}
