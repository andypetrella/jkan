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

    def send_http(self, path):
        import requests
        v = requests.get(self.URL+path, cookies=self.cookie,verify=False)
        print("[timing]["+path+"] "+str(v.elapsed.total_seconds()))
        return v.json()

    def url_for_ds(self, uuid):
        return self.URL+"/data_source_details/"+uuid


    def get_datasources(self):
       return self.send_http("/api/services/v1/resources/datasources")

    def get_datasource(self, dsId):
       return self.send_http("/api/services/v1/resources/datasource/"+dsId)

    def get_schema(self, schemaId):
       return self.send_http("/api/services/v1/resources/schema/"+schemaId)

    def _wl(self, f, s):
        f.write(s)
        f.write("\n")
        def wl2(s2): 
            return self._wl(f, s2)
        return wl2

    def create_file(self, ds):    
        data = self.get_datasource(ds['uuid'])
        
        naming = data['name'].replace('/','-').replace(' ','-').replace(':','-')
        
        with open("_datasets/"+naming+".md","w+") as md_file:
            fields = []
            for any_schema in self.get_datasource(ds['uuid'])['schemas']:
                schemaID = any_schema['schemaId']
                structure = self.get_schema(schemaID)

                for field in structure['schema']:
                    if field['columnName'] not in fields:
                        fields.append(field['columnName'])
                        

            schema ='['+','.join(fields)+']'
            title = naming
            organization = 'Lab'
            notes = 'Used in '+str(data['incomingLineages']+data['outgoingLineages'])+' lineage(s)'
            name = naming
            format = data['format']
            url = self.url_for_ds(ds['uuid'])

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
                pds = {}
                for element in ds[el]:
                    pds[element['uuid']] = self.get_datasource(element['uuid'])

                
                for element in ds[el]:
                
                    for any_schema in pds[element['uuid']]['schemas']:
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
                    data = pds[element['uuid']]
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
                    data = pds[element['uuid']]
                    name = data['name']
                    format = data['format']
                    url = self.url_for_ds(element['uuid'])
                    
                    self._wl(md_file, '  - name: '+name+' \n    url: '+url+' \n    format : '+format)

                self._wl(md_file, 'schema_fields: '+schema) \
                                    ('category:'+'\n  - Loan Acceptance Product') \
                                    ('maintainer:'+' User') \
                                    ('maintainer_email:'+' UserMail') \
                                    ("---")
            # FIXME FOR THE DEMO - PERF - WE STOP AFTER THE FIRST SCHEMA
            # break

def work(dam, cache):
    dss = dam.get_datasources() 

    project_lab = []
    project_prod = []

    for ds in dss:
        if cache.contains(ds['location']):
            print("Skipping " + ds['location'])
            continue
        else:
            if 'Loan Acceptance Product' in ds['location']:
                project_lab.append(ds)
                cache.add(ds['location'])
            elif 'demodb' in ds['location']:
                project_lab.append(ds)
                cache.add(ds['location'])
            elif 'LoanApproval' in ds['location']:
                if '2020' not in ds['location']:
                    project_lab.append(ds)
                elif '2020' in ds['location']:
                    project_prod.append(ds)
                print("Including " + ds['location'])
                cache.add(ds['location'])
            else:
                print("Excluding " + ds['location'])

    prod = {}
    for ds in project_prod:
        if ds['location'].split('/')[-1] not in prod.keys():
            prod[ds['location'].split('/')[-1]] = []
        prod[ds['location'].split('/')[-1]].append(ds)

    for ds in project_lab:
        dam.create_file(ds)

    dam.create_file_multiple(prod)

    import subprocess
    subprocess.call(["git", "pull", "origin", "gh-pages"])
    subprocess.call(["git", "add", "."])
    subprocess.call(["git", "commit", "-m", "update catalog"])
    subprocess.call(["git", "push", "origin", "gh-pages"])

class Cache(object):
    def __init__(self):
        self.cache_path = ".cache.txt"
        self.cache = set()
        try:
            with open(self.cache_path, "r") as c:
                for l in c.readlines():
                    self.cache.add(l.strip())
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
        try:
            print("Running at " + str(time.ctime()))
            work(dam, cache)
            print("Done at " + str(time.ctime()))
            period_in_seconds = 10
        except(e):
            pass
        threading.Timer(period_in_seconds, run).start()
    run()

if __name__ == "__main__":
    main()