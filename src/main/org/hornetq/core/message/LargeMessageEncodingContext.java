package org.hornetq.core.message;

import org.hornetq.core.remoting.spi.HornetQBuffer;

import java.nio.ByteBuffer;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Nov 2, 2009
 */
public interface LargeMessageEncodingContext
{
   void open() throws Exception;

   void close() throws Exception;

   int write(ByteBuffer bufferRead) throws Exception;

   int write(HornetQBuffer bufferOut, int size);
}
