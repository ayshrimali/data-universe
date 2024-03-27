import json
import re
import praw
import pymongo
import difflib
import markdownify
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy.utils.project import get_project_settings
from spiders.reddit_post_crawler import PostCrawlerSpider


# Initialize Reddit instance
reddit = praw.Reddit(
    client_id="0wR8IuNvy6TU_nMO_S62Tw",
    client_secret="rON6VGWWvZqhtc5eRgiGTye9HivqDQ",
    user_agent="Temp scraping",
)
scraped_data = []


def tokenize(s):
    return re.split('\s+', s)

def find_string_diff(s1, s2):
    l1 = tokenize(s1)
    l2 = tokenize(s2)
    str_cmd_list = []
    for op, i1, i2, j1, j2 in difflib.SequenceMatcher(a=l1, b=l2).get_opcodes():
        if op != 'equal':
            str_cmd = f"{op}: {l2[i1:i2]} --> {l1[j1:j2]}"
            print(f"str_cmd: {str_cmd} ---- {i1} {i2} {j1} {j2}")
            str_cmd_list.append(str_cmd)
    
    return str_cmd_list

def item_callback(item):
    scraped_data.append(item)
    print("data_stored_in_scraped_data: ", len(scraped_data))


def crawler_runner():
    try:
        process = CrawlerProcess(get_project_settings())
        dispatcher.connect(item_callback, signal=signals.item_scraped)
        process.crawl(PostCrawlerSpider)
        process.start()
    except Exception as e:
        print(f"Error_in_crawler_runner: {e}")


def mongo_conenction():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["reddit_strings"]
    mycollection = mydb["reddit_compare_data"]
    return mycollection


try:
    ## Run scrapy crawler
    crawler_runner()
    reddit_list = []

    ## Fetch the submission
    for post in scraped_data:
        # print(f"post: {post}")
        submission = reddit.submission(url=post["url"])
        actual_content = submission.selftext
        print(f"actual_content: {submission.url} {actual_content}")

        ## Modify reddit post to match with reddit praw format
        markdown_text = markdownify.markdownify(post["body"])

        markdown_text_1 = markdown_text.replace("\n \n\n\n\n\n\n", "\n\n&#x200B;\n\n  \n")

        pattern = re.compile(r"(\\u[a-f0-9]{4})+\n\s{1}\n{3}\s{1}")

        markdownify_fixed = pattern.sub("\n\n", markdown_text_1.strip())

        url_pattern = r"(https?://[^\s]+)"
        markdownify_fixed_2 = re.sub(url_pattern + r"(>)", r"\1", markdownify_fixed)

        scrapy_markdownify_fixed = (
            markdownify_fixed_2
            .replace("** <", " ")
            .replace("<", "")
        )
        scrapy_markdownify_fixed = scrapy_markdownify_fixed.strip()


        ## Check if the selftext matches the converted Markdowns
        compare_result = False
        if actual_content == scrapy_markdownify_fixed:
            print("String validated!!")
            compare_result = True
        else:
            print("Strings do not match.")

        reddit_dict = {
            "url": post["url"],
            "subreddit": post["subreddit-prefixed-name"],
            # "created-timestamp": post["created-timestamp"],
            'scrapy_post': post['body'],
            "scrapy_markdownified": markdown_text,
            "mondified_str": scrapy_markdownify_fixed,
            "reddit_praw": actual_content,
            # "string_diff": find_string_diff(actual_content, scrapy_markdownify_fixed),
            "is_str_validated": compare_result,
        }
        reddit_list.append(reddit_dict)

        ## Store data into mongodb
        # db_collection = mongo_conenction()
        # db_collection.insert_one(reddit_dict)

    json_object = json.dumps(reddit_list, indent=4)

    with open("scarpy_practice/scarpy_practice/reddit_compare.json", "w") as f:
        f.write(json_object)


except Exception as e:
    print(f"Error occurred: {e}")
