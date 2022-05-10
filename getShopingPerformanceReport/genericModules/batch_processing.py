import random
import re
import time
import os
import sys
from urllib.request import urlopen

MAX_POLL_ATTEMPTS = 5
PENDING_STATUSES = ('ACTIVE', 'AWAITING_FILE')

def BulkMutateInitializer(client, customer_id, operations, API_VERSION):
    BULK_MUTATE_LIMIT = 5000
    new_operations_set = []
    if len(operations)>0:
      print('Number of operations : '+str(len(operations)))
      for operation in operations:
          new_operations_set.append(operation)
          if len(new_operations_set)>BULK_MUTATE_LIMIT:
            print('partial update..')
            BulkMutate(client, customer_id, new_operations_set, API_VERSION)
            new_operations_set = []
    if len(new_operations_set)>0:
      print('final update..')
      BulkMutate(client, customer_id, new_operations_set, API_VERSION)
    else:
        print('Zero operations')

def BulkMutate(client, customer_id, operations, API_VERSION):
    # Initialize BatchJobHelper.
    client.client_customer_id = customer_id
    batch_job_helper = client.GetBatchJobHelper()
    # Create a BatchJob.
    batch_job = AddBatchJob(client, API_VERSION)
    # Retrieve the URL used to upload the BatchJob operations.
    upload_url = batch_job['uploadUrl']['url']
    batch_job_id = batch_job['id']
    print('Created BatchJob with ID "%d", status "%s", and upload URL "%s"' \
          % (batch_job['id'], batch_job['status'], upload_url))
    # Upload operations.
    batch_job_helper.UploadOperations(upload_url, operations)
    # Download and display results.
    download_url = GetBatchJobDownloadUrlWhenReady(client, batch_job_id)
    response = urlopen(download_url).read()
    PrintResponse(batch_job_helper, response)

def AddBatchJob(client, API_VERSION):
    """Add a new BatchJob to upload operations to.
    Args:
    client: an instantiated AdWordsClient used to retrieve the BatchJob.
    Returns:
    The new BatchJob created by the request.
    """
    # Initialize appropriate service.
    batch_job_service = client.GetService('BatchJobService', version=API_VERSION)
    # Create a BatchJob.
    batch_job_operations = [{
      'operand': {},
      'operator': 'ADD'
    }]
    return batch_job_service.mutate(batch_job_operations)['value'][0]

def GetBatchJob(client, batch_job_id):
  """Retrieves the BatchJob with the given id.

  Args:
    client: an instantiated AdWordsClient used to retrieve the BatchJob.
    batch_job_id: a long identifying the BatchJob to be retrieved.
  Returns:
    The BatchJob associated with the given id.
  """
  batch_job_service = client.GetService('BatchJobService')

  selector = {
      'fields': ['Id', 'Status', 'DownloadUrl'],
      'predicates': [
          {
              'field': 'Id',
              'operator': 'EQUALS',
              'values': [batch_job_id]
          }
      ]
  }

  return batch_job_service.get(selector)['entries'][0]


def GetBatchJobDownloadUrlWhenReady(client, batch_job_id,max_poll_attempts=MAX_POLL_ATTEMPTS):
    """Retrieves the downloadUrl when the BatchJob is complete.

    Args:
    client: an instantiated AdWordsClient used to poll the BatchJob.
    batch_job_id: a long identifying the BatchJob to be polled.
    max_poll_attempts: an int defining the number of times the the BatchJob will
      be checked to determine whether it has completed.

    Returns:
    A str containing the downloadUrl of the completed BatchJob.

    Raises:
    Exception: If the BatchJob hasn't finished after the maximum poll attempts
      have been made.
    """
    batch_job = GetBatchJob(client, batch_job_id)
    poll_attempt = 0
    while (poll_attempt in range(max_poll_attempts) and batch_job['status'] in PENDING_STATUSES):
        sleep_interval = (10 * (2 ** poll_attempt) +
                          (random.randint(0, 10000) / 1000))
        print(('Batch Job not ready, sleeping for %s seconds.' % sleep_interval))
        time.sleep(sleep_interval)
        batch_job = GetBatchJob(client, batch_job_id)
        poll_attempt += 1
        print(batch_job)
        if 'downloadUrl' in batch_job and batch_job['downloadUrl']:
          url = batch_job['downloadUrl']['url']
          print(('Batch Job with Id "%s", Status "%s", and DownloadUrl "%s" ready.'
                 % (batch_job['id'], batch_job['status'], url)))
          return url
    raise Exception('Batch Job not finished downloading. Try checking later.')


def PrintResponse(batch_job_helper, response_xml):
    """Prints the BatchJobService response.

    Args:
    batch_job_helper: a BatchJobHelper instance.
    response_xml: a string containing a response from the BatchJobService.
    """
    response = batch_job_helper.ParseResponse(response_xml)
    absentLocation=[]
    if 'rval' in response['mutateResponse']:
        for data in response['mutateResponse']['rval']:
          if 'errorList' in data:
           try:
            print(('Operation %s - FAILURE:' % data['index']))
            print(('\terrorType=%s' % data['errorList']['errors']['ApiError.Type']))
            print(('\ttrigger=%s' % data['errorList']['errors']['trigger']))
            print(('\terrorString=%s' % data['errorList']['errors']['errorString']))
            print(('\tfieldPath=%s' % data['errorList']['errors']['fieldPath']))
            print(('\treason=%s' % data['errorList']['errors']['reason']))

           except:
               print('Succesful but can not print')
          if 'result' in data:
            try:
                print(('Operation %s - SUCCESS.' % data['index']))
            except:
                print('Successful')
