/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.factories;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

/**
 * 
 * A ObjectMessageFactory.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class ObjectMessageFactory extends AbstractMessageFactory
{
   private static final long serialVersionUID = -8665223704967440412L;

   public Message getMessage(Session sess, int size) throws JMSException
   {
      return sess.createObjectMessage(getBytes(size));
   }
}
