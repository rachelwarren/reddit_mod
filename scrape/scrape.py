
import praw
import pandas as pd
import datetime as dt
from ast import literal_eval
class Scrape:
    def __init__(self, auth_map):
        PERSONAL_USE_SCRIPT = str(auth_map.get('PERSONAL_USE_SCRIPT'))
        SECRET_KEY_27_CHARS = str(auth_map.get('SECRET_KEY_27_CHARS'))
        APP_NAME = str(auth_map.get('APP_NAME'))
        USER_NAME = str(auth_map.get('USER_NAME'))
        PASSWORD = str(auth_map.get('PASSWORD'))

        self.reddit = praw.Reddit(client_id=PERSONAL_USE_SCRIPT,
                                  client_secret=SECRET_KEY_27_CHARS,
                                  user_agent=APP_NAME,
                                  username=USER_NAME,
                                  password=PASSWORD)

    def top_subreddit(self, sub_name, limit=10):
        subreddit = self.reddit.subreddit(sub_name)
        subreddit.contributor()
        posts = subreddit.top(limit=limit)
        return posts

    def post_to_df(self, posts):
        #TODO: this is pretty slow way of doing this, would prob. be better to append rows
        topics_dict = {"title": [],
                       "author": [],
                       "score": [],
                       "id": [],
                       "url": [],
                       "comms_num": [],
                       "created": [],
                       "body": [],
                       "comment_author": [],
                       "comment_id": []
                       }

        for submission in posts:
            sub_object = self.reddit.submission(id = submission.id)

            topics_dict["title"].append(submission.title)
            topics_dict['author'].append(submission.author)

            topics_dict["score"].append(submission.score)
            topics_dict["id"].append(submission.id)
            topics_dict["url"].append(submission.url)
            topics_dict["comms_num"].append(submission.num_comments)
            topics_dict["created"].append(submission.created)
            topics_dict["body"].append(submission.selftext)
            sub_object.comments.replace_more(limit=10)
            comment_author = [c.author.name if c.author is not None else "none" for c in sub_object.comments]
            comment_ids = [c.id if c.id is not None else "none" for c in sub_object.comments]
            topics_dict["comment_author"].append([comment_author])
            topics_dict["comment_id"].append([comment_ids])
        return pd.DataFrame(topics_dict)


    def user_sub_info(self, user_id):
        user = self.reddit.redditor(user_id)

        subreddits = {}
        comments = user.comments.new(limit = 10)

        for i in comments:
            sub_name = i.subreddit.display_name
            v = subreddits.get(sub_name)
            v = 0 if v is None else v
            subreddits[sub_name] = v + 1

        return subreddits

    def write_subredits(self, subreddit_name, base_dir):
        top_sub = self.top_subreddit(subreddit_name)
        top_sub_df = self.post_to_df(top_sub)
        top_sub_df.to_csv(base_dir + "/" + subreddit_name + "_top_posts.csv")
        return top_sub_df

subreddit_name = "Coronavirus"
f = open('reddit/auth.txt')
auth_map = literal_eval(f.read())
f.close()
r = Scrape(auth_map)
r.write_subredits(subreddit_name, "reddit/output")
top_sub_df = pd.read_csv("reddit/output/Coronavirus_top_posts.csv")
user_df = top_sub_df.groupby("author").agg(author=('author', 'first'),
                                           first_post=('created', 'min'),
                                           last_post=('created', 'max'),
                                           post_number=('id', 'count'))
user_df['sub_reddit_info'] = user_df['author'].apply(lambda x: r.user_sub_info(x))
user_df.to_csv('reddit/output/' + subreddit_name + '_posters.csv')
comment_authors = pd.DataFrame(
    {'author': top_sub_df['comment_author'].apply(lambda x: pd.Series(literal_eval(x)[0])).stack()})

comment_authors['sub_reddit_info'] = comment_authors['author'].apply(lambda x: r.user_sub_info(x))
comment_authors.to_csv("reddit/output/" + "/" + subreddit_name + '_comenters.csv')


