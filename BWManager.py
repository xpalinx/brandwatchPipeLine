import time

from bcr_api.bwproject import BWProject, BWUser
from bcr_api.bwresources import BWQueries, BWGroups
import Utils


class BWManager:
    def __init__(self):
        self.username = "jlozano@phyx.co"
        self.password = "Phyxbrand2023+"
        self.token_path = "tokens.txt"
        self.YOUR_ACCOUNT = "jlozano@phyx.co"
        self.YOUR_PROJECT = "PHYX"
        self.project = None

    def login(self):
        offline = True
        while offline:
            try:
                BWUser(username=self.username, password=self.password, token_path=self.token_path)
                self.project = BWProject(username=self.YOUR_ACCOUNT, project=self.YOUR_PROJECT)
                offline = False
            except Exception as e:
                print(e)
                time.sleep(600)

    def download_query_data_to_df(self, query_name, start, end):
        downloaded = False
        while not downloaded:
            try:
                queries = BWQueries(self.project)
                jsonobj = queries.get_mentions(name=query_name,
                                               startDate=start.strftime("%Y-%m-%d") + "T05:00:00",
                                               endDate=end.strftime("%Y-%m-%d") + "T05:00:00")
                print("downloading query from ", start.strftime("%Y-%m-%d") + "T05:15:00", " to ",
                      end.strftime("%Y-%m-%d") + "T05:15:00")
                return Utils.json_to_df(jsonobj)
            except Exception as e:
                print(e)
                time.sleep(150)

    def download_group_data_to_df(self, query_name, start, end):
        downloaded = False
        while not downloaded:
            try:
                groups = BWGroups(self.project)
                jsonobj = groups.get_mentions(name=query_name,
                                              startDate=start.strftime("%Y-%m-%d") + "T05:00:00",
                                              endDate=end.strftime("%Y-%m-%d") + "T05:00:00")
                print("downloading query from ", start.strftime("%Y-%m-%d") + "T05:00:00", " to ",
                      end.strftime("%Y-%m-%d") + "T05:00:00")
                return Utils.json_to_df(jsonobj)
            except Exception as e:
                print(e)
                time.sleep(150)
