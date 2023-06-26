import os
import logging
from time import sleep
import firebase_admin
from firebase_admin import firestore
import functions_framework
from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed


@functions_framework.http
def device_sync(request):
    import requests
    api_key = os.environ.get('API_KEY')
    url = os.environ.get('FITBIT_SYNC_DOMAIN')

    try:
        r = requests.get('%s/ingest' % (url), headers={'API_KEY': api_key})
        r.raise_for_status()
        if r.ok:
            logging.info('Retrieved requested data')
    except requests.exceptions.HTTPError as e:
        logging.error(e.strerror)
    except requests.exceptions.Timeout:
        logging.warn("Request failed due to timeout. Attempt again")
        for i in range(5):
            sleep(i*2)
            try:
                r = requests.get('%s/ingest' % (url), headers={'API_KEY': api_key})
                logging.info("Request succeeded after try %s" % (str(i)))
                break
            except requests.exceptions.Timeout as e:
                logging.warn("Request failed again, try %s out of 5" % (str(i)))
                if i == 5:
                    logging.error("Unable to send request due to timeout " + e.strerror)
    except requests.exceptions.TooManyRedirects as e:
        logging.error("Number of redirects exceeded threshold: " + e.strerror)
    except requests.exceptions.RequestException as e:
        logging.error("Request contains an error:" + e.strerror)
        SystemExit(e)
        raise SystemExit(e)
    return "Finished pulling Fitbit data", 200


@functions_framework.http
def device_async(request):
    api_key = os.environ.get('API_KEY')
    url = os.environ.get('FITBIT_SYNC_DOMAIN')
    if not firebase_admin._apps:
        firebase_admin.initialize_app()
    firestore_client = firestore.client()
    fitbitdb = firestore_client.collection(os.environ.get('FIRESTORE_DATASET'))
    url_lst, usr_lst = [], []
    for doc in fitbitdb.stream():
        if 'user_id' in doc.to_dict():
            print('Created request for user %s' % doc.id)
            url_lst.append("%s/ingest" % (url))
            usr_lst.append(doc.id)
    with FuturesSession() as session:
        futures = [session.get(url, headers={'API_KEY': api_key, 'user': user}) for url, user in zip(url_lst, usr_lst)]

        for future in as_completed(futures):
            future.result()
    return "Finished pulling Fitbit data", 200
