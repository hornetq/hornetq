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
package org.jboss.test.messaging.jms.stress;

import javax.jms.Session;

/**
 * 
 * A Runner.
 * 
 * Base class for running components of a stress test
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * Runner.java,v 1.1 2006/01/17 12:15:33 timfox Exp
 */
public abstract class Runner implements Runnable
{
   protected Session sess;
   
   protected int numMessages;
   
   protected boolean failed;

   public Runner(Session sess, int numMessages)
   {
      this.sess = sess;
      this.numMessages = numMessages;
   }
   
   public abstract void run();
   
   public boolean isFailed()
   {
      return failed;
   }

}
