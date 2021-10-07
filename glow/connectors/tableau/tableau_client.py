import requests
import xml.etree.ElementTree as ET
import re 

TABLEAU_VERSION = '3.13'

class TableauException(Exception):
    pass

class TableauClient():

    token = None
    site_id = None

    server = None
    sitename = None
    username = None
    password = None

    url = None

    def __init__(self, server, sitename, username, password) -> None:
        self.server = server
        self.sitename = sitename
        self.username = username
        self.password = password
        self.url = '{server}api/{version}/'.format(server=server, version=TABLEAU_VERSION)

    def _auth(self):
        data = """
        <tsRequest>
	        <credentials name="{username}" password="{password}">
		        <site contentUrl="{sitename}" />
	        </credentials>
        </tsRequest>
        """.format(username=self.username, password=self.password, sitename=self.sitename)
        
        r = requests.post(url='{url}/auth/signin'.format(url=self.url), data=data)
        
        if r.status_code != 200:
            raise Exception('Could not authenticate')
        
        root = ET.fromstring(r.text)
        self.token = root[0].attrib['token']
        self.site_id = root[0][0].attrib['id']
    
    def _auth_decorator(func):
        def wrapper(self):
            if self.token is None or self.site_id is None:
                self._auth()
            return func(self)
        return wrapper


    def _get_tasks_from_xml(self, xml):
        if xml[0].tag != 'tasks':
            raise TableauException('tasks could not be found in XML.')

        tasks = []

        for task in xml[0]:
            new_task = {}
            if task[0].tag == 'extractRefresh':
                new_task['id'] = task[0].attrib['id']
                new_task['type'] = task[0].attrib['type']
                if task[0][0].tag == 'schedule':
                    new_task['schedule_id'] = task[0][0].attrib['id']
                    new_task['frequency'] = task[0][0].attrib['frequency']
                    new_task['state'] = task[0][0].attrib['state']
                    new_task['next_run_at'] = task[0][0].attrib['nextRunAt']
                new_task['target_type'] = task[0][1].tag
                new_task['target_id'] = task[0][1].attrib['id']
            tasks.append(new_task)
        return tasks

    @_auth_decorator
    def get_tasks(self):
        url = '{url}/sites/{site}/tasks/extractRefreshes'.format(url=self.url,
                                                                               version=TABLEAU_VERSION,
                                                                               site=self.site_id)
        headers = {'X-Tableau-Auth': self.token}
        r = requests.get(url=url, headers=headers)
        
        if r.status_code != 200:
            raise Exception('Could not find tasks')
        
        # quick hack to remove xml namespace
        xml_text = re.sub(' xmlns="[^"]+"', '', r.text, count=1)
        root = ET.fromstring(xml_text)
        return self._get_tasks_from_xml(root)
