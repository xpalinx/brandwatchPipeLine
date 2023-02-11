from bcr_api.bwproject import BWProject, BWUser
from bcr_api.bwresources import BWQueries, BWGroups, BWAuthorLists, BWSiteLists, BWLocationLists, BWTags, BWCategories, \
    BWRules, BWMentions, BWSignals
import datetime
import logging

import Utils


class BWManager:
    def __init__(self):
        self.username = ""
        self.password = ""
        self.token_path = "tokens.txt"
        self.YOUR_ACCOUNT = ""
        self.YOUR_PROJECT = "PHYX"
        self.project = None

    def login(self):
        logger = logging.getLogger("bcr_api")

        BWUser(username=self.username, password=self.password, token_path=self.token_path)
        self.project = BWProject(username=self.YOUR_ACCOUNT, project=self.YOUR_PROJECT)

    def download_query_data_to_df(self, query_name, start, end):
        for attempt in range(10):
            try:
                queries = BWQueries(self.project)
                jsonobj = queries.get_mentions(name=query_name,
                                               startDate=start.strftime("%Y-%m-%d") + "T05:00:00",
                                               endDate=end.strftime("%Y-%m-%d") + "T05:00:00")
                print("downloading query from ", start.strftime("%Y-%m-%d") + "T05:15:00", " to ",
                      end.strftime("%Y-%m-%d") + "T05:15:00")
                return Utils.json_to_df(jsonobj)
            except Exception as e:
                self.login()
            else:
                break
    def download_group_data_to_df(self, query_name, start, end):
        for attempt in range(10):
            try:
                groups = BWGroups(self.project)
                jsonobj = groups.get_mentions(name=query_name,
                                              startDate=start.strftime("%Y-%m-%d") + "T05:00:00",
                                              endDate=end.strftime("%Y-%m-%d") + "T05:00:00")
                print("downloading query from ", start.strftime("%Y-%m-%d") + "T05:00:00", " to ",
                      end.strftime("%Y-%m-%d") + "T05:00:00")
                return Utils.json_to_df(jsonobj)
            except Exception as e:
                self.login()
            else:
                break
