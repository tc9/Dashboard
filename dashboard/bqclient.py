import httplib2
from apiclient.discovery import build
from oauth2client.appengine import oauth2decorator_from_clientsecrets

class BigQueryClient(object):
    def __init__(self, http, decorator):
        """Creates the BigQuery client connection"""
        self.service = build('bigquery', 'v2', http=http)
        self.decorator = decorator

    def getTableData(self, project, dataset, table):
        # The credentials must already exist before you call decorator.http()
        # So you cannot pre-generate 'decorated' in the BigQueryClient constructor,
        # only from within a method protected by .oauth_required
        decorated = self.decorator.http()
        return self.service.tables().get(projectId=project, datasetId=dataset,
            tableId=table).execute(decorated)

    def getLastModTime(self, project, dataset, table):
        data = self.getTableData(project, dataset, table)
        if data is not None and 'lastModifiedTime' in data:
            return data['lastModifiedTime']
        else:
            return None

    def Query(self, query, project, timeout_ms=10000):
        query_config = {
            'query': query,
            'timeoutMs': timeout_ms
        }
        decorated = self.decorator.http()
        result_json = (self.service.jobs()
                       .query(projectId=project, body=query_config)
                       .execute(decorated))

        return result_json