/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.asyncio.impl;

import java.nio.ByteBuffer;

import org.jboss.messaging.core.asyncio.AIOCallback;
import org.jboss.messaging.core.asyncio.AsynchronousFile;
import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * @author clebert.suconic@jboss.com
 * Warning: Case you refactor the name or the package of this class
 *          You need to make sure you also rename the C++ native calls
 */
public class JlibAIO implements AsynchronousFile
{
    private static Logger log = Logger.getLogger(JlibAIO.class);
    private boolean opened = false;
    private String fileName;
    private Thread poller;
    private static boolean loaded = true;
    
    /**
     *  Warning: Beware of the C++ pointer! It will bite you! :-)
     */ 
    private long handler;
    
    static
    {
        try
        {
            log.info("JLibAIO being loaded");
            System.loadLibrary("JBMLibAIO");
        }
        catch (Throwable e)
        {
            log.error(e.getLocalizedMessage(), e);
            loaded = false;
        }
    }
    
    public static boolean isLoaded()
    {
       return loaded;
    }
    
    

    
    public void open(String fileName, int maxIO)
    {
        opened = true;
        this.fileName=fileName;
        handler = init (fileName, AIOCallback.class, maxIO, log);
        startPoller();
    }
    
    class PollerThread extends Thread
    {
        PollerThread ()
        {
            super("NativePoller for " + fileName);
        }
        public void run()
        {
            pollEvents();
        }
    }
    
    private void startPoller()
    {
        checkOpened();
        poller = new PollerThread(); 
        poller.start();
    }
    
    public void close()
    {
        checkOpened();
        opened = false;
        closeInternal(handler);
        handler = 0;
    }
    
    
    public void write(long position, long size, ByteBuffer directByteBuffer, AIOCallback aioPackage)
    {
        checkOpened();
        write (handler, position, size, directByteBuffer, aioPackage);
        
    }

    public void read(long position, long size, ByteBuffer directByteBuffer, AIOCallback aioPackage)
    {
        checkOpened();
        read (handler, position, size, directByteBuffer, aioPackage);
        
    }

    public long size()
    {
        checkOpened();
        // TODO: wire this method to ftell
        return 0;
    }

    public void preAllocate(int blocks, long size)
    {
        checkOpened();
        preAllocate(handler, blocks, size);
    }

    private void pollEvents()
    {
        checkOpened();
        internalPollEvents(handler);
    }
    
    private void checkOpened() 
    {
        if (!opened)
        {
            throw new RuntimeException("File is not opened");
        }
    }
    
    /** 
     * I'm sending aioPackageClazz here, as you could have multiple classLoaders with the same class, and I don't want the hassle of doing classLoading in the Native layer
     */
    @SuppressWarnings("unchecked")
    private static native long init(String fileName, Class aioPackageClazz, int maxIO, Logger logger);
    
    private static native void write(long handle, long position, long size, ByteBuffer buffer, AIOCallback aioPackage);

    private static native void read(long handle, long position, long size, ByteBuffer buffer, AIOCallback aioPackage);
    
    private static native void preAllocate(long handle, int blocks, long size);

    private static native void closeInternal(long handler);
    
    /** Poll asynchrounous events from internal queues */
    private static native void internalPollEvents(long handler);

    // Should we make this method static?
    public native void destroyBuffer(ByteBuffer buffer);

    // Should we make this method static?
    public native ByteBuffer newBuffer(long size);
   
    
}
