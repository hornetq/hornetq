package org.hornetq.core.remoting.impl.netty;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

public interface ConnectionCreator extends ChannelHandler
{
   NettyServerConnection createConnection(final ChannelHandlerContext ctx, String protocol, boolean httpEnabled) throws Exception;
}
