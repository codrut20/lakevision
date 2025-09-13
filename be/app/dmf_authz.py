import requests
import os
from fastapi import Request, Response, status

class DMFAuthz(object):

    def __init__(self):
        self.token = os.environ.get('PYICEBERG_CATALOG__DEFAULT__TOKEN')
        cat_url = os.environ.get('PYICEBERG_CATALOG__DEFAULT__URI')
        self.url = cat_url.rstrip("/iceberg")

    def make_api_call(self, endpoint: str, params: dict = {}):
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.token}"}
        # sending post request and saving response as response object
        response = requests.get(url=f'{self.url}/{endpoint}', params=params, headers=headers) #, verify=False

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(response.text)

    def has_access(self, request: Request, response: Response, table_id):
        user = request.session.get("user")
        if not user: #authentication is not enabled otherwise user would be in session
            return True
        name = table_id.rsplit(".", 1)
        namespace = name[0]
        table = name[1]
        endpoint=f'authz/namespace/{namespace}/table/{table}/user/{user}'
        res = self.make_api_call(endpoint)
        print(res)
        if res['permission'] == 'read' or res['permission'] == 'write':
            return True
        response.status_code = status.HTTP_403_FORBIDDEN
        return False

    def get_teams_with_owners(self):
        endpoint = 'authz/namespace/team_with_owners'
        return self.make_api_call(endpoint)

    def get_namespace_special_properties(self, namespace):
        teams = self.get_teams_with_owners()
        for item in teams:
            if isinstance(item, dict) and item.get('namespace') == namespace:
                return f"Namespace owner(s): {item['owners']}"

    """
        returns {
            "restricted": bool
        }
    """
    def get_table_special_properties(self, table_id):
        name = table_id.rsplit(".", 1)
        namespace = name[0]
        table = name[1]
        endpoint = f"/authz/namespace/{namespace}/table/{table}/public_read_access"
        res = self.make_api_call(endpoint)
        return {"restricted": not res['restricted']} #the api returs inverted value