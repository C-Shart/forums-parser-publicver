import config
import time
import re
import regexes as r
from logma import Logger
from pandas import DataFrame
from page_requestors import PageGetters, UserCheck
from tableflip import SqlTools
from parsers import Parsers, Randoms
from time import sleep

logger = Logger.logger("crawlers", "info" )
timer = Logger.timer("timers")

class Crawlers:
    def __init__(self) -> None:
        self.sql = SqlTools()
        self.pg = PageGetters()
        self.parse = Parsers()
        logger.info("")
        logger.info("")
        logger.info(f"==================================")
        logger.info(f"Initializing spiderman.Crawlers...")
        logger.info(f"==================================")

    def last_page_check(self, page):
        lp = not re.search("title=\"Next page\"", page)
        return lp

    def add_forumid(self, fid):
        current_fids = set(self.sql.get_table_cols("forums", ["forum_id"]))
        if fid in current_fids:
            print(f"Forum id {fid} already exists")
            logger.debug(f"Forum {fid} not added: Already exists")
            pass
        else:
            fid_soup = self.pg.get_forumdisplay(fid)
            forumdisplay_data = self.parse.parse_forumdisplay(fid_soup)
            df_forums = forumdisplay_data["forums"].set_index("forum_id")
            df_subforums = DataFrame() if forumdisplay_data["subforums"].empty else forumdisplay_data["subforums"].set_index("sub_id")
            self.sql.upsert_df(df_forums, "forums")
            print(f"forumid {fid} added to table goons.forums")
            logger.debug(f"Forum {fid} added to goons.forums")
            if df_subforums.empty:
                pass
            else:
                self.sql.upsert_df(df_subforums, "subforums")
                logger.debug(f"Subforums for forum {fid} added to goons.subforums.")
        # c_sleep = Randoms.click_sleep()
        # sleep(c_sleep)

    def generate_indexes(self):
        with open ("docs/forum_ids.txt") as file:
            initial_fid_list = [fid.rstrip() for fid in file]
        logger.info("Beginning to build forum index tables goons.forums and goons.subforums...")
        for fid in initial_fid_list:
            self.add_forumid(fid)
            logger.debug(f"Forum {fid} added to goons.forums")
        subs_list = self.sql.get_table_cols("subforums", ["sub_id"])["sub_id"]
        for fid in subs_list:
            self.add_forumid(fid)
            logger.debug(f"Forum {fid} added to goons.subforums")
        logger.info("Forum index tables goons.forums and goons.subforums have been populated!")

    def backfill_users_gangtags(self):
        with open ("docs/backfilled_gts.txt") as file:
            dones = set([int(fid.rstrip()) for fid in file])
        all_uids = set(self.sql.get_table_cols("users", ["userid"])["userid"].tolist())
        db_gts = set(self.sql.get_table_cols("users_gangtags", ["userid"])["userid"].tolist())
        bf_uids = all_uids.difference(db_gts)
        working_uids = bf_uids.difference(dones)
        for uid in working_uids:
            logger.info(f"BACKFILL: Checking user {uid} for gangtags users_gangtags")
            self.crawl_profile(uid)
        return None

    def backfill_post_links_threads(self):
        logger.info("BACKFILL: Beginning backfill for post_links_threads...")
        backfill_pages = []
        bf_pids = self.sql.get_table_cols("post_links_thread", ["post_id"], condition=f"threadid IS NULL AND pageno IS NULL AND userid IS NULL AND postid IS NULL").drop_duplicates()["post_id"].tolist()
        for pid in bf_pids:
            t_query = self.sql.get_table_cols("post_meta", ["thread_id","page"], condition=f"post_id={pid}")
            tid_pages = (t_query.thread_id.values[0], t_query.page.values[0])
            backfill_pages.append(tid_pages)
        logger.info(f"BACKFILL: {len(backfill_pages)} pages to parse...")
        for tid,page in set(backfill_pages):
            self.crawl_single_page(tid, page_no=page)
        logger.info(f"BACKFILL: COMPLETE for post_links_threads")
        return None

    def crawl_subforum(self, subforum_id:int, page_no=1):
        page_check = self.pg.get_subforum_page(subforum_id)
        last_page = self.parse.get_last_subforum_page(page_check)

        logger.info(f"Checking forum {subforum_id} p{page_no} for new threads...")

        is_complete = False
        while page_no <= last_page:
            this_page = self.pg.get_subforum_page(subforum_id, page_no)
            new_threads = self.parse.get_new_threads(this_page)
            print(f"Parsing page {page_no} of forum {subforum_id}")
            is_complete = bool(len(new_threads)==0)
            page_dataframe = self.parse.parse_subforum(this_page)
            if page_dataframe.empty:
                logger.info(f"Subforum {subforum_id} has no threads, skipping...")
                break
            if is_complete:
                logger.info(f"Threadlist crawling completed (or close enough) for forum {subforum_id}, skipping.")
                break
            df_users = page_dataframe["thread_author"].tolist()
            self.add_users_if_missing(df_users)
            self.sql.upsert_df(page_dataframe, "threads")
            logger.info(f"Page {page_no} of forum {subforum_id} appended to goons.threads")
            page_no += 1
            sleep(Randoms.random_uniform())
        print(f"COMPLETE: Crawling complete for forum {subforum_id}")
        logger.info(f"COMPLETE: Crawling complete for forum {subforum_id}")
        return None

    def populate_subforums(self) -> None:
        db_forum_list = Randoms.shuffle(self.sql.get_table_cols("forums", ["forum_id"])["forum_id"].tolist())
        for fid in db_forum_list:
            self.crawl_subforum(fid)

    def add_users_if_missing(self, ids:[]) -> None:
        db_return = self.sql.get_table_cols("users", ["userid"]).values.tolist()
        db_list = [user for row in db_return for user in row]
        missings = [id for id in ids if int(id) not in db_list]
        with open("docs/users.txt", 'a') as wr:
            wr.writelines(missings)

        # Disabling due to presence of member.php in robots.txt
        # for uid in set(missings):
        #    self.crawl_profile(uid)
        return

    def crawl_new_threads(self, fid):
        page = 1
        is_last_page = False
        is_complete = False
        while not is_last_page or not is_complete:
            logger.info(f"New thread check: Forum {fid} page {page}...")
            this_page = self.pg.get_subforum_page(fid, p=page)
            is_last_page = self.last_page_check(this_page)
            new_threads = self.parse.get_new_threads(this_page)
            is_complete = bool(len(new_threads) == 0)
            if is_complete:
                logger.info(f"New thread check: No new threads found in forum {fid}")
                return None
            for tid in new_threads:
                logger.info(f"New thread found since last run: {tid}")
                # TODO: add thread to threads
                self.crawl_thread(tid)
            page += 1
        return None

    def crawl_single_page(self, thread_id, page_no=1):
        this_page = self.pg.get_thread_page(thread_id, page_no)
        page_dataframe = self.parse.parse_posts(this_page)
        if not page_dataframe.empty:
            self.sql.upsert_df(page_dataframe, "post_meta")
            print(f"Appended posts from page {page_no} of thread {thread_id} to post tables. Checking users...")
            post_users = page_dataframe["userid"].tolist()
            self.add_users_if_missing(post_users)
            print(f"COMPLETE: Single page parsed: Thread {thread_id} page {page_no}")
        sleep(Randoms.random_uniform())
        return

    def crawl_thread(self, thread_id, page_no=1):
        click_sleep = Randoms.random_uniform()
        is_last_page = False
        while not is_last_page:
            start = time.time()
            timer.info(f"{thread_id} | Requesting page...")
            this_page = self.pg.get_thread_page(thread_id, page_no)
            is_last_page = self.last_page_check(this_page)
            page_dataframe = self.parse.parse_posts(this_page)
            if not page_dataframe.empty:
                self.sql.upsert_df(page_dataframe, "post_meta")
                print(f"Appended posts from page {page_no} of thread {thread_id} to post tables. Checking users...")
                post_users = page_dataframe["userid"].tolist()
                self.add_users_if_missing(post_users)
                page_no += 1
            end = time.time()
            elapsed = end - start
            if elapsed < click_sleep:
                sleeptime = click_sleep - elapsed
                timer.info(f"{thread_id} | SLEEPING: {sleeptime}")
                sleep(sleeptime)
            else:
                timer.info(f"{thread_id} | PARSE TIME: {elapsed}")
        return print(f"COMPLETE: Crawling complete for thread {thread_id}")

    def populate_post_tables(self) -> None:
        db_tids = self.sql.get_table_cols("threads", ["thread_id"])["thread_id"].tolist()

        pig_lists = []
        with open ("docs/pigs.txt", "r") as pigs:
            pig_lists = [int(x) for x in pigs.read().split("\n")]

        working_list = set(db_tids) - set(pig_lists)

        for tid in working_list:
            logger.info(f"Checking thread {tid}...")
            this_page = self.pg.get_thread_page(tid)
            r_lastpage = re.search(r.re_lastpage, this_page)
            lpc = None if not r_lastpage else r_lastpage.group(1)
            lastpage_value = None if not lpc else int(lpc)
            if not lastpage_value:
                logger.info(f"BEGINNING CRAWL OF SINGLE PAGE THREAD {tid}")
                self.crawl_thread(tid)
                continue
            db_pages = self.sql.get_table_cols("post_meta", ["page"], condition=f"thread_id={tid}")
            start_page = db_pages.max()["page"] if not db_pages.empty else 1
            logger.info(f"BEGINNING CRAWL OF THREAD {tid} FROM PAGE {start_page}")
            self.crawl_thread(tid, start_page)
        return print(f"COMPLETE! Post tables have been fully populated.")

    def crawl_archive_page(self, fid, page, year):
        logger.info(f"ARCHIVES | {year} | Forum {fid} | ENTERING ARCHIVES")
        self.pg.enter_archives(fid, year=year)
        logger.info(f"ARCHIVES | {year} | Forum {fid} | Crawling archived threads...")
        self.crawl_subforum(fid, page)
        self.pg.exit_archives()
        return

    def crawl_archives(self, fid):
        for year in range(2015, 2001, -1):
            logger.info(f"ARCHIVES | {year} | {fid} | Entering archives...")
            self.pg.enter_archives(fid, year=year)
            logger.info(f"ARCHIVES | {year} | {fid} | Crawling archived threads...")
            self.crawl_subforum(fid)
        logger.info(f"ARCHIVES | {year} | {fid} | CRAWL COMPLETE")
        self.pg.exit_archives()
        return

    def populate_archives(self) -> None:
        db_forum_list = Randoms.shuffle(self.sql.get_table_cols("forums", ["forum_id"])["forum_id"].tolist())
        for fid in db_forum_list:
            self.crawl_archives(fid)
        logger.info(f"ARCHIVES | All archives crawled. Exiting.")
        return

    def all_tids_in_db(self, page) -> bool:
        new_threads = self.parse.get_new_threads(page)
        is_complete = bool(len(new_threads)==0)
        return is_complete

    # commenting out due to robots.txt
    def crawl_profile(self, userid):
        """ uc = UserCheck()
        user_exists = uc.check_if_user_real(userid=userid)
        if not user_exists:
            logger.info(f"User {userid} not found! Skipping...")
        else:
            profile = self.pg.get_profile(userid)
            df_profile = self.parse.parse_profile(profile)
            if not df_profile.empty:
                self.sql.upsert_df(df_profile, "users")
                print(f"Userid {userid} appended to goons.users")
        sleep(Randoms.random_uniform()) """
        return

    def crawl_leper(self, ):
        self.sql.get_table_cols("leper", cols=[])

    def progress_check(self, ):

        pass

