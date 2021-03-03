from mdutils.mdutils import MdUtils
from mdutils import Html

class DAM(object):
    def __init__(self):
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        import os
        self.URL = os.getenv("URL")
        self.PAT = os.getenv("PAT")

        self.cookie = self.get_cookie()


    def get_cookie(self):
        import requests
        session = requests.Session()
        cookieurl=self.URL + '/api/auth/callback?client_name=ExternalAppTokenClient'
        header = {'X-External-App-Token':self.PAT}
        response = session.post(url=cookieurl,headers=header,verify=False)
        cookie = session.cookies
        return cookie

    def get_datasources(self):
        import requests
        v = requests.get(self.URL+"/api/services/v1/resources/datasources", cookies=self.cookie,verify=False)
        return v.json()

    def get_datasource(self, dsId):
        import requests
        v = requests.get(self.URL+"/api/services/v1/resources/datasource/%s" %dsId,cookies=self.cookie,verify=False)
        return v.json() 

    def get_schema(self, schemaId):
        import requests
        v = requests.get(self.URL+"/api/services/v1/resources/schema/%s" %schemaId,cookies=self.cookie,verify=False)
        return v.json()

    def _wl(self, f, s):
        f.write(s)
        f.write("\n")
        def wl2(s2): 
            return self._wl(f, s2)
        return wl2

    def create_file(self, ds):    
        data = self.get_datasource(ds['uuid'])
        
        naming = data['name'].replace('/','-')
        
        with open("_datasets/"+naming+".md","w+") as md_file:
            fields = []
            for any_schema in self.get_datasource(ds['uuid'])['schemas']:
                schemaID = any_schema['schemaId']
                structure = self.get_schema(schemaID)

                for field in structure['schema']:
                    if field['columnName'] not in fields:
                        fields.append(field['columnName'])
                        

            schema ='['+','.join(fields)+']'
            title = data['name']
            organization = 'Lab'
            notes = 'Used in '+str(data['incomingLineages']+data['outgoingLineages'])+' lineage(s)'
            name = data['name']
            format = data['format']
            url = data['location']

            self._wl(md_file, "---") \
                            ("schema: chicago") \
                            ("title: "+title) \
                            ("organization: "+organization) \
                            ("notes: "+notes) \
                            ("resources:") \
                            ('  - name: '+name+' \n    url: '+url+' \n    format : '+format) \
                            ('schema_fields: '+schema) \
                            ('category:'+'\n  - Loan Acceptance Product') \
                            ('maintainer:'+' User') \
                            ('maintainer_email:'+' UserMail') \
                            ("---")
    
    def create_file_multiple(self, ds):
        fields = []
        for el in ds:
            naming = el
            with open("_datasets/"+naming+".md","w+") as md_file:

                for element in ds[el]:
                
                    for any_schema in self.get_datasource(element['uuid'])['schemas']:
                        schemaID = any_schema['schemaId']
                        structure = self.get_schema(schemaID)

                        for field in structure['schema']:
                            if field['columnName'] not in fields:
                                fields.append(field['columnName'])


                schema = '['+','.join(fields)+']'
                
                title = el
                
                organization = 'Production'
                
                income = 0
                outcome = 0
                
                for element in ds[el]:
                    data = self.get_datasource(element['uuid'])
                    income += data['incomingLineages']
                    outcome+= data['outgoingLineages']
                notes = 'Used in '+str(income+outcome)+' lineage(s)'
                
                self._wl(md_file, "---") \
                                    ("schema: chicago") \
                                    ("title: "+title) \
                                    ("organization: "+organization) \
                                    ("notes: "+notes) \
                                    ("resources:")
                
                for element in ds[el]:
                    data = self.get_datasource(element['uuid'])
                    name = data['name']
                    format = data['format']
                    url = data['location']
                    
                    self._wl(md_file, '  - name: '+name+' \n    url: '+url+' \n    format : '+format)

                self._wl(md_file, 'schema_fields: '+schema) \
                                    ('category:'+'\n  - Loan Acceptance Product') \
                                    ('maintainer:'+' User') \
                                    ('maintainer_email:'+' UserMail') \
                                    ("---")

def work(dam, cache):
    dss = dam.get_datasources() 

    project_lab = []
    project_prod = []

    count = 0
    for ds in dss:
        if cache.contains(ds['location']):
            continue
        cache.add(ds['location'])
        if 'LoanApproval' in ds['location']:
            if '2020' not in ds['location']:
                project_lab.append(ds)
            elif '2020' in ds['location']:
                project_prod.append(ds)
            count += 1
            if count == 10:
                break

    prod = {}
    for ds in project_prod:
        if ds['location'].split('/')[-1] not in prod.keys():
            prod[ds['location'].split('/')[-1]] = []
        prod[ds['location'].split('/')[-1]].append(ds)

    for ds in project_lab:
        dam.create_file(ds)

    dam.create_file_multiple(prod)

class Cache(object):
    def __init__(self):
        self.cache_path = ".cache.txt"
        self.cache = set()
        try:
            with open(self.cache_path, "r") as c:
                for l in c.readlines():
                    self.cache.add(l)
        except FileNotFoundError as e:
            pass

    def add(self, s):
        self.cache.add(s)
        with open(self.cache_path, "a+") as c:
            c.write(s+"\n")
    def contains(self, s):
        return s in self.cache

import time, threading
def main():
    dam = DAM()
    cache = Cache()
    def run():
        print("Running at " + str(time.ctime()))
        work(dam, cache)
        print("Done at " + str(time.ctime()))
        period_in_seconds = 10
        threading.Timer(period_in_seconds, run).start()
    run()

if __name__ == "__main__":
    main()