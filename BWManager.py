from bcr_api.bwproject import BWProject, BWUser
from bcr_api.bwresources import BWQueries, BWGroups, BWAuthorLists, BWSiteLists, BWLocationLists, BWTags, BWCategories, \
    BWRules, BWMentions, BWSignals
import datetime
import logging
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
        logger = logging.getLogger("bcr_api")

        # (Default) All logging messages enabled
        # logger.setLevel(logging.DEBUG)

        # Does not report URLs of API requests, but all other messages enabled
        # logger.setLevel(logging.INFO)

        # Report only errors and warnings
        # logger.setLevel(logging.WARN)

        # Report only errors
        # logger.setLevel(logging.ERROR)

        # Disable logging
        # logger.setLevel(logging.CRITICAL)

        BWUser(username=self.username, password=self.password, token_path=self.token_path)
        self.project = BWProject(username=self.YOUR_ACCOUNT, project=self.YOUR_PROJECT)

    def download_query_data_to_df(self, query_name, start, end):
        queries = BWQueries(self.project)
        start = (datetime.date.today() - datetime.timedelta(days=start)).isoformat() + "T05:15:00"
        today = (datetime.date.today() - datetime.timedelta(days=end)).isoformat() + "T05:15:00"
        jsonobj = queries.get_mentions(name=query_name,
                                       startDate=start,
                                       endDate=today)
        return Utils.json_to_df(jsonobj)

    def first_last_day_of_month(month, year):
        first_day_of_month = datetime.datetime(year, month, 1)

        if month == 12:
            last_day_of_month = datetime.datetime(year + 1, 1, 1) - datetime.timedelta(days=1)
        else:
            last_day_of_month = datetime.datetime(year, month + 1, 1) - datetime.timedelta(days=1)

        return first_day_of_month, last_day_of_month

    def download_group_data_to_df(self, group_name, month, day, year):
        first_day, last_day = self.first_last_day_of_month(month)
        groups = BWGroups(self.project)
        if day:
            start = (datetime.date.today() - datetime.timedelta(days=1)).isoformat() + "T05:15:00"
            today = (datetime.date.today() + datetime.timedelta(days=1)).isoformat() + "T05:15:00"
        else:
            start = (first_day + "T05:15:00")
            today = (last_day + "T05:15:00")
        jsonobj = groups.get_mentions(name=group_name,
                                      startDate=start,
                                      endDate=today)
        return Utils.json_to_df(jsonobj)




