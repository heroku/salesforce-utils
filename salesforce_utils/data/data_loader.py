__author__ = 'spersinger'
import os
import sys
import pdb
import json
import yaml
from datetime import datetime

import requests
from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
from salesforce_utils import SalesforceBatch
import record_generator

def gen_Contact(count):
    fields = [
        "AssistantName__c=>name name",
        "Birthdate__c=>date",
        "Department__c=>titlewords|2",
#        "Description__c=>titlewords|4",
        "Description__c=>counter",
        "DoNotCall__c=>bool",
        "EmailBouncedDate__c",
        "EmailBouncedReason__c=>words|2",
        "FirstName__c",
        "HasOptedOutOfEmail__c=>bool",
        "LastName__c",
        "Email__c",
        "Phone__c",
        "MailingCity__c",
        "MailingCountry__c=>US",
        "MailingState__c",
        "MailingStreet__c=>int|5 street",
        "MailingPostalCode__c=>int|5",
        "OwnerId=>UserId",
    ]

    return record_generator.mock_records(fields, count=count)

def gen_Account(count):
    fields = [
        "AccountNumber=>string|40",
        "OwnerId=>UserId",
        "AnnualRevenue=>float",
        "BillingCity=>city",
        "BillingCountryCode=>country",
        "BillingLatitude=>float|1",
        "BillingLongitude=>float|1",
        "BillingPostalCode=>int|5",
        "BillingState=>state",
        "BillingStreet=>int|5 street",
        "CustomBool1__c=>bool",
        "CustomBool2__c=>bool",
        "CustomDate1__c=>date",
        "CustomDate2__c=>datetime",
        "CustomDate3__c=>datetime",
        "CustomFloat1__c=>float",
        "CustomInt1__c=>int",
        "CustomInt2__c=>int",
        "CustomPhone1__c=>phone",
        "CustomPhone2__c=>phone",
        "CustomString1__c=>string|250",
        "CustomString2__c=>string|250",
        "CustomString3__c=>string|220",
        "CustomString6__c=>string|4000",
        "CustomUrl1__c=>website",
        "CustomUrl2__c=>website",
        "Description=>titlewords|8",
        "Industry=>Industry",
        "Name=>titlewords|3",
        "NumberOfEmployees=>int|5",
        "Phone=>phone",
        "ShippingCity=>city",
        "ShippingCountryCode=>country",
        "ShippingPostalCode=>int|5",
        "ShippingState=>state",
        "ShippingStreet=>int|5 street",
        "Type=>account_type",
        "Website=>website"
    ]
    return record_generator.mock_records(fields, count=count)



def load_records(test=False, target="Contact1000__c", count=10, batch_size=100000,
                username="scottp+test@heroku.com", password=None, token=None,
                sessionId=None, endpoint=None,
                return_records=False, field_spec = None):
    if not test:
        if username and password:
            sf = SalesforceBatch(username=username, password=password, token=token)
        else:
            sf = SalesforceBatch(sessionId=sessionId, endpoint=endpoint)

        user_ids = [r.Id for r in sf.query_salesforce("User", ["Id"], where="ReceivesAdminInfoEmails=true", limit=20).records]
        print "User ids: " + str(user_ids)

        bulk = SalesforceBulk(sessionId=sf.sessionId, host=sf.host)

        job = bulk.create_insert_job(target, concurrency="Parallel")
    else:
        user_ids = [1, 2, 3]

    record_generator.define_lookup("UserId", random_choices=user_ids)
    record_generator.define_lookup("Industry", random_choices=["Finance","Agriculture","Technology","Banking","Chemicals"])
    record_generator.define_lookup("account_type", random_choices=["Analyst","Competitor","Customer","Integrator","Partner"])


    global indexer
    indexer = 0

    def gen_index():
        global indexer
        indexer += 1
        return "{0} {1}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), indexer)

    record_generator.define_lookup("counter", callable=gen_index)

    output = open("records_{0}.json".format(target), "a")

    total = count
    batches = []
    all_records = []

    while count > 0:
        if field_spec:
            records = record_generator.mock_records(field_spec, count=batch_size)
        else:
            if 'Contact' in target:
                records = gen_Contact(min(count,batch_size))
            else:
                records = gen_Account(min(count,batch_size))
        if test:
            return list(records)

        print "Made batch of size {}".format(batch_size)

        if return_records:
            records = list(records)
            all_records += records
            records = iter(records)

        if total < 1000:
            # Use SOAP
            sf.insert_salesforce(target, records)
            count = 0
        else:
            csv_gen = CsvDictsAdapter(records)

            print "Posting batch to BULK API"
            batch = bulk.post_bulk_batch(job, csv_gen)
            print "Posted: %s" % batch
            batches.append(batch)
            count -= batch_size

        for r in records:
            output.write(json.dumps(r))
            output.write("\n")

    for b in batches:
        print "Waiting for %s" % b
        bulk.wait_for_batch(job, b)

    bulk.close_job(job)

    print "DONE!"
    if return_records:
        return all_records

if __name__=='__main__':
    if len(sys.argv) < 2:
        print "Usage: data_loader <spec file> [<count>]"
        print "Without a count a test record will be printed"
    else:
        spec = yaml.load(open(sys.argv[1]))
        os.environ['SALESFORCE_CLIENT_ID'] = str(spec['sf_client_id'])
        os.environ['SALESFORCE_CLIENT_SECRET'] = str(spec['sf_client_secret'])
        os.environ['SALESFORCE_REDIRECT_URI'] = str(spec['sf_client_redirect'])

        test=False
        try:
            count = int(sys.argv[2])
        except IndexError:
            test = True
            count = 1

        res = load_records(test=test, target=spec['object'], count=count, batch_size=3000,
                username=spec['username'], password=spec['password'], token=spec['token'],
                field_spec=spec['fields'])
        if test:
            for record in res:
                print record
                print "Record length: {}".format(len(json.dumps(record)))

