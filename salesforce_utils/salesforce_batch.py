import re
import time
import yaml
import random
import os.path
import urlparse
import subprocess
from datetime import datetime
from collections import defaultdict

from sforce.partner import SforcePartnerClient

import salesforce_oauth_request



# Makes it easy to generate updates/inserts to a target system.
# insert_salesforce will take a YAML string, allowing things like this:
#
#   yaml = """
#           -
#             name: scott
#           -
#             name: mher
#          """
#   ug.insert_salesforce("Contact", yaml)

class SalesforceBatch(object):
    def __init__(self, tenant = None, sessionId=None, endpoint=None, 
                 username=None, password=None, token=None, partner_wsdl=None):

        self.tenant = tenant
        self.sfclient = None
        self.db = None

        if partner_wsdl is None:
            partner_wsdl = os.path.join(os.path.dirname(__file__), "partnerwsdl.xml")

        if username and password:
            login = salesforce_oauth_request.login(username=username, password=password, 
                                                    token=token, cache_session=True)
            if 'access_token' in login:
                sessionId = login['access_token']
                endpoint = login['endpoint']
            else:
                raise RuntimeError("SF Login error: {0}".format(login))

        if sessionId:
            """This is the trick to use an OAuth access_token as the session id in the SOAP client."""
            self.h = SforcePartnerClient(partner_wsdl)

            self.h._sessionId = sessionId
            self.h._setEndpoint(endpoint)
            header = self.h.generateHeader('SessionHeader')
            header.sessionId = sessionId
            self.h.setSessionHeader(header)
            self.sfclient = self.h

    @property
    def sessionId(self):
        return self.h._sessionId

    @property
    def endpoint(self):
        return self.h._location

    @property
    def host(self):
        o = urlparse.urlparse(self.h._location)
        return o.hostname.replace("-api","")

    def sf_client(self):
        """

        @return: SforcePartnerClient
        """
        # if not self.sfclient:
        #     system = self.tenant.default_salesforce
        #     sfadapter = SFAdapter.init_from_system(system)
        #     sfadapter.login()
        #     self.sfclient = sfadapter.h

        return self.sfclient

    def query_salesforce(self, object_name, fields=["Id"], limit = 200, where=None):
        client = self.sf_client()
        clauses = ["IsDeleted = False"] if object_name != "User" else []
        if where:
            clauses.append(where)

        if len(clauses) > 0:
            where = "WHERE %s" % " AND ".join(clauses)
        else:
            where = ""

        return client.queryAll("SELECT %s from %s %s LIMIT %d" % (",".join(fields), object_name, where, limit))

    def query_salesforce_id_set(self, object_name, count, where=None):
        client = self.sf_client()

        qOpts = client.generateHeader('QueryOptions')
        qOpts.batchSize = 200
        client.setQueryOptions(qOpts)

        clauses = [] if object_name == 'User' else ["IsDeleted = False"]
        if where:
            clauses.append(where)

        if len(clauses) > 0:
            where = "WHERE %s" % " AND ".join(clauses)
        else:
            where = ""

        results = client.queryAll("SELECT Id from %s %s LIMIT %d" % (object_name, where, count))
        ids = [r.Id for r in results.records if hasattr(r, 'Id')]
        while not results.done:
            results = client.queryMore(results.queryLocator)
            ids += [r.Id for r in results.records if hasattr(r, 'Id')]

        return ids

    def batches(self, items):
        for x in range(0, len(items), 200):
            yield items[x:x+200]

    def batch_client(self, service, items):
        client = self.sf_client()
        results = []
        for tranch in self.batches(items):
            results += getattr(client, service)(tranch)

        return results

    def update_salesforce(self, object_name, **kwargs):
        client = self.sf_client()

        limit = kwargs.pop('limit', 10)
        where = kwargs.pop('where', None)
        updates = []

        for id in self.query_salesforce_id_set(object_name, limit, where=where):
            record = client.generateObject(object_name)
            record.Id = id
            for field, val in kwargs.iteritems():
                setattr(record, field, val)
            updates.append(record)

        uresult = self.batch_client('update', updates)
        self.show_results(uresult)

    def show_results(self, sf_results):
        try:
            if not isinstance(sf_results, list):
                sf_results = [sf_results]
            updated_ids = [r.id for r in sf_results if hasattr(r, 'id')]

            print "Updated: %s" % updated_ids

            err_results = [r for r in sf_results if not r.success]
            msgs = [er.errors[0].message for er in err_results]
            if len(msgs) > 0:
                print "WARNING, SF UPDATE RETURNED ERRORS:"
                print msgs
        except:
            print sf_results

    def insert_salesforce(self, object_name, values_list):
        if isinstance(values_list, basestring):
            values_list = yaml.load(values_list)
        client = self.sf_client()

        inserts = []
        for values in values_list:
            record = client.generateObject(object_name)
            for field, val in values.iteritems():
                setattr(record, field, val)
            inserts.append(record)

        results = self.batch_client('create', inserts)
        self.show_results(results)

    def delete_salesforce(self, object_name, ids_or_count, where=None):
        client = self.sf_client()

        if isinstance(ids_or_count, (int,long)):
            ids_or_count = self.query_salesforce_id_set(object_name, ids_or_count, where=where)

        if isinstance(ids_or_count, list):
            results = self.batch_client('delete', ids_or_count)
            self.show_results(results)

        else:
            raise ValueError("Invalid arg ids_or_count")


    def copy_object(self, source, target):
        fields = subprocess.check_output(("force field list %s" % source).split())
        fields = fields.split("\n")
        bads = ['reference','email','date','id']
        good = []
        for f in fields:
            if len([t for t in bads if f.startswith(t) or f.endswith(t) or (t+" ") in f]) > 0:
                print "Skipping field: %s" % f
            else:
                good.append(f)
        fields = good
        fields = ["%s:%s" % (m.group(1), m.group(3)) if m else "" for m in (re.match(r"(\w+?)(__c)?:\s*(\w+)", f) for f in fields)]
        cmd = "force sobject create %s %s" % (target, fields[0])
        print cmd
        try:
            print subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError, e:
            print e

        for f in fields[1:]:
            cmd = "force field create %s__c %s" % (target, f)
            print cmd
            try:
                print subprocess.check_output(cmd, shell=True)
            except subprocess.CalledProcessError, e:
                print e


class AutoUpdater:
    """Automatically sends a stream of updates to a Salesforce org. Good for ongoing integration testing.
    THIS CLASS WILL DESTROY THE DATA IN YOUR ORG - USE ON A TESTING ORG ONLY.
    """


    def __init__(self, *record_types, **kwargs):
        """Send interval keyword to set minutes between update batches. Must provide tenant= kwarg"""
        self.record_types = record_types
        self.interval = kwargs.get('interval', 15)
        self.inserts = defaultdict(int)
        self.updates = defaultdict(int)
        self.deletes = defaultdict(int)
        self.updater = UpdateGenerator(kwargs['tenant'])
        self.accounts = self.updater.query_salesforce("Account").records
        self.words =  set(open(kwargs['words']).read().split())
        contacts = open(kwargs['contacts']).read().split("\n")
        fields = contacts[0].split(",")
        self.contacts = []
        for line in contacts[1:]:
            record = eval("list([%s])" % line)
            self.contacts.append(dict(zip(fields, record)))

    def run(self):
        while True:
            self.iteration()
            print "Sleeping for %d minutes" % self.interval
            time.sleep(self.interval * 60)


    def iteration(self):
        """Randomly generate updates to the org. Every self.interval will send a new set of updates, a combination
        of inserts/updates/deletes to the indicated record types."""
        for x in xrange(10):
            choice = random.random()
            record_type = self.choose_random_record_type()
            if choice < 0.1:
                self.generate_delete(record_type)
                self.deletes[record_type] += 1
            elif choice < 0.7:
                self.generate_update(record_type)
                self.updates[record_type] += 1
            else:
                self.generate_insert(record_type)
                self.inserts[record_type] += 1

        for record_type in self.record_types:
            print "[%s] %12s: ins: %d, upd: %d, del: %d" % (datetime.now(), record_type, self.inserts[record_type],
                                                    self.updates[record_type], self.deletes[record_type])

        print "Totals: ins: %d, upd: %d, del: %d" % (sum(self.inserts.values()), sum(self.updates.values()), sum(self.deletes.values()))

    def choose_random_record_type(self):
        return self.record_types[random.randint(0, len(self.record_types)-1)]

    def generate_insert(self, record_type):
        self.updater.insert_salesforce(record_type, [self.create_insert_record(record_type)])

    def generate_description(self, record_type):
        return "Test %s: %s" % (record_type, str(datetime.now()))

    def create_insert_record(self, record_type):
        if record_type == "Contact":
            record = random.sample(self.contacts, 1)[0]
            record2 = random.sample(self.contacts, 1)[0]
            record['FirstName'] = record2["FirstName"]
            record['MailingStreet'] = record.pop('Address')
            record['MailingCity'] = record.pop('City')
            record['MailingState'] = record.pop('State')
            record['MailingPostalCode'] = record.pop('ZIP')
            record.pop('Company')
            record.pop('County')
            record.pop('Web')
            account = random.sample(self.accounts, 1)[0]
            record['AccountId'] = account.Id

        elif record_type == "Account":
            name = " ".join(random.sample(self.words, 3))
            record = {"Name": name}
            record['Description'] = "Test account, %s" % str(datetime.now())
            contact = random.sample(self.contacts, 1)[0]
            record['Website'] = contact['Web']

        elif record_type == 'Lead':
            contact = random.sample(self.contacts,  1)[0]
            contact2 = random.sample(self.contacts, 2)[0]
            record = {"FirstName" : contact["FirstName"], "LastName": contact2["LastName"], "Email":contact["Email"]}
            record["Phone"] = contact2["Phone"]
            record['Description'] = "Test lead, %s" % str(datetime.now())
            record['Company'] = contact2['Company']

        record['Description'] = self.generate_description(record_type)

        return record

    def generate_update(self, record_type):
        pass

    def create_update_record(self, record_type):
        if record_type == "Contact":
            record = {"Phone": str("%10d" % random.randint(1111111111, 9999999999))}
        elif record_type == "Lead":
            contact = random.sample(self.contacts, 1)[0]
            record = {"Email": contact["Email"]}
        elif record_type == "Acount":
            record = {"Fax": str("%10d" % random.randint(1111111111, 9999999999))}

        record['Description'] = self.generate_description(record_type)

        return record

    def generate_delete(self, record_type):
        self.updater.delete_salesforce(record_type, 1)


