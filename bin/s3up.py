#!/usr/bin/env python
# coding=utf-8
#
# s3up.py
# 2010-2011, Mike Tigas
# http://mike.tig.as/]
#
# Usage:
# s3up filename [bucket] [remote_path]
#    Uploads the given file to the specified bucket.
#    If no bucket is specified, defaults to scalr-ops-archive.
#
# s3up filename [bucket] [remote_path]
# s3up filename [bucket] [remote_path]
#
#
#
# Please set the following options below before using:
#   DEFAULT_BUCKET
#   UPLOAD_PARALLELIZATION
#   CHUNK_SIZE
#   CHUNK_RETRIES

import os
import sys
import traceback
from mimetypes import guess_type
from datetime import datetime, timedelta
from time import sleep
from urllib import urlencode
from boto.s3.connection import S3Connection
from cStringIO import StringIO
from threading import Thread
from math import ceil

if not os.environ.has_key('AWS_ACCESS_KEY_ID') or not os.environ.has_key('AWS_SECRET_ACCESS_KEY'):
    print "Error: must set environment variables AWS_ACCESS_KEY_ID and"
    print "AWS_SECRET_ACCESS_KEY."
    print
    sys.exit(1)

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# S3 endpoint for Northern Virginia
S3_ENDPOINT = "s3-external-1.amazonaws.com"

# When only given one or two args, the following bucket is used:
DEFAULT_BUCKET = 'default'

# Number of simultaneous upload threads to execute.
UPLOAD_PARALLELIZATION = 20

# Minimum size for a file chunk (except final chunk). Needs to be >= 5242880.
CHUNK_SIZE = 10485760

# Maximum number of chunks (limitation of S3 multipart uploads)
MAX_CHUNKS = 10000

# For robustness, we can retry uploading any chunk up to this many times. (Set to
# 1 or less to only attempt one upload per chunk.) Because we chunk large uploads,
# an error in a single chunk doesn't necessarily mean we need to re-upload the
# entire thing.
CHUNK_RETRIES = 10

# ========== "MultiPart" (chunked) upload utility methods ==========

def mem_chunk_file(local_file):
    """
    Given the file at `local_file`, returns a generator of CHUNK_SIZE
    (default 5MB) StringIO file-like chunks for that file.
    """
    global CHUNK_SIZE
    fstat = os.stat(local_file)
    fsize = fstat.st_size
    
    num_chunks = int(ceil(float(fsize) / float(CHUNK_SIZE)))
    if num_chunks > MAX_CHUNKS:
        # File would require too many chunks. Make each chunk larger instead.
        CHUNK_SIZE = int(ceil(float(fsize) / float(MAX_CHUNKS)))
        CHUNK_SIZE = int(1024.0 * ceil(float(CHUNK_SIZE) / 1024.0))  # round up to nearest 1KB
        num_chunks = int(ceil(float(fsize) / float(CHUNK_SIZE)))
    
    fp = open(local_file, 'rb')
    for i in range(num_chunks):
        if i == (num_chunks-1):
            size_hint = fsize - (CHUNK_SIZE * (num_chunks - 1))
        else:
            size_hint = CHUNK_SIZE
        
        tfp = StringIO()
        tfp.write(fp.read(size_hint))
        tfp.seek(0)
        yield tfp
    fp.close()

def upload_worker(multipart_key, fp, index, headers=None):
    """
    Uploads a file chunk in a MultiPart S3 upload. If an error occurs uploading
    this chunk, retry up to `CHUNK_RETRIES` times.
    """
    success = False
    attempts = 0
    while not success:
        try:
            fp.seek(0)
            multipart_key.upload_part_from_file(fp, index, headers=headers)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            success = False
            
            attempts += 1
            if attempts >= CHUNK_RETRIES:
                break
            
            sleep(0.5)
        else:
            success = True
    
    if not success:
        raise Exception("Upload of chunk %d failed after 5 retries." % index)
    
    fp.close()

def upload_chunk(arg_list):
    thread = Thread(
        target=upload_worker,
        args=arg_list
    )
    thread.daemon = False
    thread.start()
    return thread

# ========== Uploader methods ==========

def easy_up(local_file):
    if os.path.isfile(local_file):
        print "File:"
        print os.path.abspath(local_file)
        print

        remote_path = os.path.basename(local_file)

        upload_file(os.path.abspath(local_file), DEFAULT_BUCKET, remote_path, 0)

        print "File uploaded to:"
        print "https://%s/%s/%s" % (S3_ENDPOINT, DEFAULT_BUCKET, remote_path)
        
        print
    else:
        print "Path given is not a file."

def upload_file(local_file, bucket, remote_path, force_download=False):
    # Metadata that we need to pass in before attempting an upload.
    content_type = guess_type(local_file, False)[0] or "application/octet-stream"
    basic_headers = {
        "Content-Type" : content_type,
    }
    if force_download:
        basic_headers["Content-Disposition"] = "attachment; filename=%s"% os.path.basename(local_file)
    
    # Set up a connection to S3
    s3 = S3Connection(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      host=S3_ENDPOINT)
    bucket = s3.get_bucket(bucket)
    
    # Get info on the local file to determine whether it's large enough that we can perform
    # upload parallelization.
    fstat = os.stat(local_file)
    fsize = fstat.st_size
    
    # Can only chunk in increments of 5MB, so if the file size is smaller than that, fall back to
    # the "standard" upload procedure.
    if fsize <= CHUNK_SIZE:
        #print "Standard upload: File size is under %.1f MB\n" % (CHUNK_SIZE/1024**2)
        key = bucket.new_key(remote_path)
        key.content_type = content_type
        key.set_contents_from_filename(local_file, headers=basic_headers)
    else:
        #print "Parallelized upload\n"
        mp_key = bucket.initiate_multipart_upload(remote_path, headers=basic_headers)
        
        try:
            # Chunk the given file into `CHUNK_SIZE` (default: 10MB) chunks that can
            # be uploaded in parallel.
            chunk_generator = mem_chunk_file(local_file)
            
            # Use `UPLOAD_PARALLELIZATION` (default: 4) threads at a time to churn through
            # the `chunk_generator` queue.
            active_threads = []
            for i, chunk in enumerate(chunk_generator):
                args = (mp_key, chunk, i+1, basic_headers)
                
                # If we don't have enough concurrent threads yet, spawn an upload thread to
                # handle this chunk.
                if len(active_threads) < UPLOAD_PARALLELIZATION:
                    # Upload this chunk in a background thread and hold on to the thread for polling.
                    t = upload_chunk(args)
                    active_threads.append(t)
                
                # Poll until an upload thread finishes before allowing more upload threads to spawn.
                while len(active_threads) >= UPLOAD_PARALLELIZATION:
                    for thread in active_threads:
                        # Kill threads that have been completed.
                        if not thread.isAlive():
                            thread.join()
                            active_threads.remove(thread)
                    
                    # a polling delay since there's no point in constantly waiting and taxing CPU
                    sleep(0.1)
            
            # We've exhausted the queue, so join all of our threads so that we wait on the last pieces
            # to complete uploading.
            for thread in active_threads:
                thread.join()
        except:
            # Since we have threads running around and possibly partial data up on the server,
            # we need to clean up before propogating an exception.
            sys.stderr.write("Exception! Waiting for existing child threads to stop.\n\n")
            for thread in active_threads:
                thread.join()
            
            # Remove any already-uploaded chunks from the server.
            mp_key.cancel_upload()
            for mp in bucket.list_multipart_uploads():
                if mp.key_name == remote_path:
                    mp.cancel_upload()
            
            # Propogate the error.
            raise
        else:
            # We finished the upload successfully. 
            mp_key.complete_upload()
            key = bucket.get_key(mp_key.key_name)
        
        # ===== / chunked upload =====

def main(args):
    if len(args) == 0 or len(args) > 3:
        print "Usage:"
        print "s3up filename [bucket] [remote_path]"
        print "    Uploads the given file to the specified bucket."
        print "    If no bucket is specified, defaults to %s." % DEFAULT_BUCKET
        print
        sys.exit(1)

    local_file = os.path.abspath(args[0])
    bucket = args[1] if len(args) >= 2 else DEFAULT_BUCKET
    remote_path = args[2] if len(args) >= 3 else os.path.basename(local_file)

    upload_file(local_file, bucket, remote_path)

if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except Exception, e:
        sys.stderr.write('\n')
        traceback.print_exc(file=sys.stderr)
        sys.stderr.write('\n')
        sys.exit(1)

