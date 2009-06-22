
#include <stdio.h>
#include <stdlib.h>
#include <libaio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>

long getTime()
{
   struct timeval time;
   if (gettimeofday(&time, 0) < 0)
   {
	   fprintf (stderr, "Error on getting time\n");
	   exit(-1);
   }

   return time.tv_sec * 1000 + time.tv_usec / 1000;

}

/**
 * Authored by Clebert Suconic @ redhat . com
 * Licensed under LGPL
 */
int main(int arg, char * param[])
{
   char * directory;
   int numberOfFiles;
   int fileSize = 10 * 1024 * 1024;
   int bufferSize = 128 * 1024;
   void * preAllocBuffer = 0;

   int maxAIO = 500;


   if (arg != 3)
   {
       fprintf (stderr, "usage disktest <directory> <numberOfFiles>\n");
       exit(-1);
   }

   directory = param[1];
   numberOfFiles = atoi(param[2]);

   if (posix_memalign(&preAllocBuffer, 512, bufferSize))
   {
       fprintf (stderr, "Error allocating buffer");
       exit(-1);
   }

   memset(preAllocBuffer, 0, bufferSize);

   fprintf (stderr, "====================================================================================\n");
   fprintf (stderr, " Step 1: preAllocate files\n");
   fprintf (stderr, "====================================================================================\n");


   long start = getTime();

   for (int i = 0 ; i < numberOfFiles; i++)
   {
      char file[1024];
      sprintf (file, "%s/file%d.dat", directory, i);
      fprintf (stderr, "creating file %s\n", file);

      long startfile = getTime();

      int handle = open (file, O_RDWR | O_CREAT | O_DIRECT, 0666);

      for (long size = 0; size < fileSize ; size += bufferSize)
      {
         if (write(handle, preAllocBuffer, bufferSize) < 0)
         {
            fprintf (stderr, "Error writing file %s\n", file);
            exit(-1);
         }
      }

      close(handle);

      long endfile = getTime();

      fprintf (stderr, "Total time to allocate file = %ld milliseconds, Bytes/millisecond = %ld\n", (endfile - startfile), (fileSize / (endfile - startfile)));

   }

   long end = getTime();

   fprintf (stderr, "Total time on allocating = %ld, Bytes/millisecond = %ld \n", end - start, (numberOfFiles * fileSize  / (end - start)));


   memset(preAllocBuffer, 1, bufferSize);
   char * tst = (char *) preAllocBuffer;
   tst[0] = 't';
   tst[1] = 'e';
   tst[2] = 's';
   tst[3] = 't';
   tst[4] = '{';
   for (int i = 5; i < bufferSize - 2; i++)
   {
       tst[i] = 'a' + (i % 20);
   }
   tst[bufferSize-1] = '}';

   fprintf (stderr, "====================================================================================\n");
   fprintf (stderr, " Step 2: write libaio\n");
   fprintf (stderr, "====================================================================================\n");


   long globalStartAIO = getTime();

   for (int i = 0 ; i < numberOfFiles; i++)
   {
      char file[1024];
      sprintf (file, "%s/file%d.dat", directory, i);
      fprintf (stderr, "writing on file %s using AIO\n", file);

      io_context_t aioContext;

      io_queue_init(maxAIO, &aioContext);

      struct io_event *events = (struct io_event *)malloc (maxAIO * sizeof (struct io_event));

      int handle = open(file,  O_RDWR | O_CREAT | O_DIRECT, 0666);

      int writes = 0; // total number of writes

      long startAIO = getTime();

      for (long position = 0 ; position < fileSize; position += bufferSize)
      {
    		writes++;
		struct iocb * iocb = new struct iocb();
		::io_prep_pwrite(iocb, handle, preAllocBuffer, bufferSize, position);
		iocb->data = (void *)position;
	
		if (io_submit(aioContext, 1, &iocb) < 0)
		{
			fprintf (stderr, "Error on submitting AIO\n");
			exit(-1);
		}
      }

      int writesReceived = 0;

      while (writesReceived < writes)
      {
    	  int result = io_getevents(aioContext, 1, maxAIO, events, 0);

    	  writesReceived += result;


    	  for (int errCheck = 0 ; errCheck < result; errCheck++)
    	  {
    		  long result = events[i].res;
    		  if (result < 0)
    		  {
    			  fprintf (stderr, "error on writing AIO\n");
    			  exit(-1);
    		  }
    		  else
    		  {
    			  struct iocb * iocbp = events[errCheck].obj;
    			  delete iocbp;
    		  }
    	  }
      }

      long endAIO = getTime();


      fprintf (stderr, "Total time to write file = %ld milliseconds, Bytes/millisecond = %ld\n", (endAIO - startAIO), (fileSize / (endAIO - startAIO)));


      free (events);
      io_queue_release(aioContext);

   }

   long globalEndAIO = getTime();


   fprintf (stderr, "Total time on write files = %ld, Bytes/millisecond = %ld \n", globalEndAIO - globalStartAIO, (numberOfFiles * fileSize  / (globalEndAIO - globalStartAIO)));




   free(preAllocBuffer);

   return (0);
}
