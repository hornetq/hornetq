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
package org.jboss.test.messaging.tools.tx;


/**
 *  The public interface of timeouts.
 *   
 *  @author <a href="osh@sparre.dk">Ole Husgaard</a>
 *  @version $Revision$
*/
interface Timeout {
   /**
    *  Cancel this timeout.
    *
    *  It is guaranteed that on return from this method this timer is
    *  no longer active. This means that either it has been cancelled and
    *  the timeout will not happen, or (in case of late cancel) the
    *  timeout has happened and the timeout callback function has returned.
    *
    *  On return from this method this instance should no longer be
    *  used. The reason for this is that an implementation may reuse
    *  cancelled timeouts, and at return the instance may already be
    *  in use for another timeout.
    */
   public void cancel();
}

