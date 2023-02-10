import pandas as pd


def json_to_df(jsonobj):
    df = pd.DataFrame(jsonobj)
    df = df.astype(str)
    return df


def split_dataframe(df, chunk_size=10000):
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i * chunk_size:(i + 1) * chunk_size])
    return chunks


def charge_sql(db, df, query_name):
    retries = 0
    chunks = split_dataframe(df)
    for c in chunks:
        print("Sending chunk")
        db.create_or_insert_table(c, query_name)
    return retries


def get_columns_bdb(df):
    return df.loc[:, ['added', 'categoryDetails', 'author', 'avatarurl', 'city', 'continent', 'country', 'region',
                      'engagementtype', 'facebookauthorid', 'facebookcomments', 'facebooklikes',
                      'facebookrole', 'facebookshares', 'facebooksubtype'
                         , 'fulltext', 'fullname', 'gender', 'guid', 'impressions',
                      'instagramcommentcount', 'instagramfollowercount', 'instagramfollowingcount',
                      'instagraminteractionscount'
                         , 'instagramlikecount', 'instagrampostcount', 'interest', 'language',
                      'linkedincomments', 'linkedinengagement', 'linkedinimpressions',
                      'linkedinlikes', 'linkedinshares'
                         , 'linkedinsponsored', 'linkedinvideoviews', 'matchpositions',
                      'mediaurls', 'monthlyvisitors', 'pagetype', 'parentpostid', 'pubtype',
                      'publishersubtype'
                         , 'queryname', 'reachestimate', 'redditscore', 'redditscoreupvoteratio',
                      'redditcomments', 'redditauthorkarma', 'redditauthorawardeekarma'
                         , 'redditauthorawarderkarma', 'resourcetype', 'rootpostid', 'sentiment',
                      'snippet', 'subreddit', 'subredditsubscribers', 'subtype'
                         , 'tags', 'threadauthor', 'threadcreated', 'threadentrytype', 'threadid',
                      'threadurl', 'title', 'twitterfollowers'
                         , 'twitterfollowing', 'twitterpostcount', 'twitterreplycount',
                      'twitterretweets', 'twitterlikecount', 'twitterverified'
                         , 'url', 'copyright', 'weblogtitle', 'pagetypename', 'contentsource',
                      'contentsourcename', 'impact', 'resourceid', 'imagemd5s', 'imageinfo'
                         , 'logoimages']]


def get_date_time(df):
    df.columns = df.columns.str.lower()
    df['Dates'] = pd.to_datetime(df['added']).dt.date
    df['Time'] = pd.to_datetime(df['added']).dt.time
    return df

