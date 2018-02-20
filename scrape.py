import praw
import pandas as pd

import progressbar

# Note: you will have to provide your own praw.ini file 
# http://praw.readthedocs.io/en/latest/getting_started/configuration/prawini.html
# for more info

class RedditScraper:
    def __init__(self, bot='bot1', subreddits = [], progress_bar=True):
        """
        Initializes our RedditScraper.
        
        Parameters
        -----------------
        bot : str
            Bot name, specified in praw.ini 

        subreddits : list
            List of strings of all desired subreddits to scrape

        progress_bar : bool
            Whether or not to display a progress bar

        """
        self.reddit = praw.Reddit(bot)
        self.subreddits = subreddits
        self.progress_bar = progress_bar

    def scrape_to_df(self, post_limit=100, comment_limit=5, file_name=None, save_period=1000):
        """ 
        Given list of subreddits in constructor, number of comments and posts, scrape comments into a dataframe.

        Parameters
        -----------------
        post_limit : int
            The number of posts to request

        comment_limit : int
            The number of comments per post to request

        file_name : str
            The desired RELATIVE file save path
            Providing None will not save the dataframe to a file.

        save_period : int
            Save after save_period submissions have been processed

        returns a pandas DataFrame
        """
        def initialize_dict(comment_limit):
            """
            Initialize an empty dict for dataframe usage.
            """
            data = {}
            for i in range(comment_limit):
                top_comment_str = "Top_comment_{0}".format(i + 1)
                data[top_comment_str] = []
                data["{0}-score".format(top_comment_str)] = []
            data["Title"] = []
            return data

        if self.progress_bar:
            bar = progressbar.ProgressBar(max_value = post_limit)

        dfs = []
        for subreddit in self.subreddits:
            subreddit_dfs = [] # We save every save_period - we save these in distinct dataframes so collect here
            data = initialize_dict(comment_limit)

            for i, submission in enumerate(self.reddit.subreddit(subreddit).top(limit=post_limit)):
                if self.progress_bar:
                    bar.update(i)

                data["Title"].append(submission.title.encode("utf-8"))
                for j, comment in enumerate(submission.comments[0:comment_limit]):
                    top_comment_str = "Top_comment_{0}".format(j + 1)
                    data[top_comment_str].append(comment.body)
                    data["{0}-score".format(top_comment_str)].append(comment.score)
                
                if file_name and save_period and i > 0 and i % save_period == 0:
                    # Save what we've collected so far
                    df = pd.DataFrame(data)
                    df.to_csv("{0}-{1}-{2}.csv".format(file_name, subreddit, int(i / save_period)))
                    subreddit_dfs.append(df)
                    data = initialize_dict(comment_limit)

            subreddit_dfs.append(pd.DataFrame(data))
            df = pd.concat(subreddit_dfs)
            df["Subreddit"] = subreddit
            if file_name and len(self.subreddits) > 1:
                df.to_csv("{0}-{1}.csv".format(file_name, subreddit), encoding="utf-8")
            dfs.append(df)

        df = pd.concat(dfs)
        if file_name:
            df.to_csv("{0}.csv".format(file_name), encoding="utf-8")
            print ("\n\n DataFrame saved to {0}.csv".format(file_name))
        return df

# Turns out that we can only get 1000 posts at a time due to the API...
rs = RedditScraper(subreddits=['nba'])
df = rs.scrape_to_df(file_name="data/nba", post_limit=1000, comment_limit=50, save_period=200)
