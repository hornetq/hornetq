/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.tests.timing.util;

import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.VariableLatch;

/**
 * @author <a href="csuconic@redhat.com">Clebert Suconic</a>
 */
public class VariableLatchTest extends UnitTestCase
{
   public void testTimeout() throws Exception
   {
      VariableLatch latch = new VariableLatch();

      latch.up();

      long start = System.currentTimeMillis();
      assertFalse(latch.waitCompletion(1000));
      long end = System.currentTimeMillis();

      assertTrue("Timeout didn't work correctly", end - start >= 1000 && end - start < 2000);
   }
}
