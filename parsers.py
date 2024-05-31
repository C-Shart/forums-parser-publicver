from logma import Logger
from time import sleep
import os, os.path
from sys import exit
import uuid
import tableflip
import re
import random
import regexes as r
import pandas as pd
from page_requestors import UserCheck
from collections import Counter
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs

logger = Logger.logger("parsers", "info")

class Randoms:
    def click_sleep():
        sleep_time = 0
        roll = 0
        roll = random.randint(1,5000)
        match roll:
            case r if r < 999:
                sleep_time = random.uniform(2.01,5.73)
            case r if 999 <= r < 1999 :
                sleep_time = random.uniform(4.21,7.35)
            case r if 1999 <= r < 3010:
                sleep_time = random.uniform(5.58,9.08)
            case r if 3010 <= r < 4000:
                sleep_time = random.uniform(1.72,26.99)
            case r if 4000 <= r < 4999:
                sleep_time = random.uniform(26.02,107.83)
            case r if 4999 <= r <= 5000:
                sleep_time = random.uniform(1504.75, 3613.13)
        print(f"Sleeping for {sleep_time:.2f}s...")
        logger.info(f"Sleeping for {sleep_time:.2f} seconds...")
        return sleep_time

    def shuffle(a_list:[]) -> []:
        random.shuffle(a_list)
        return a_list

    def random_uniform(x=5,y=6.66):
        sleep_time = random.uniform(x,y)
        logger.info(f"Sleeping for {sleep_time:.2f} seconds...")
        return sleep_time


class Parsers:
    sql = tableflip.SqlTools()

    def error_check(self, page_html) -> str:
        error_type = None

        general_error = re.search("Special Message From El Jefe", str(page_html))
        gateway_error = re.search("Bad Gateway", str(page_html))
        banned = re.search("You\'ve Been Banned", str(page_html))

        if general_error:
            error_type = "general"
        elif gateway_error:
            error_type = "gateway"
        elif banned:
            error_type = "banned"

        return error_type

    def strip_non_text(self, this_postdiv):
        this_div = re.sub(r.re_blockquote, "", this_postdiv)
        this_div = re.sub(r.re_br, "", this_div)
        this_div = re.sub("<[^:>]+>", "", this_div)
        this_div = re.sub("\n+", "", this_div)
        return this_div

    def emptyquote_check(self, this_body):
        this_body = self.strip_non_text(this_body)
        this_body = re.sub("\s+", "", this_body)
        return this_body

    def strip_quotes(self, input):
        cleaned_input = BeautifulSoup(re.sub(r.re_blockquote, "", str(input)), "lxml")
        return cleaned_input

    def find_image_embeds(self, pd):
        clean_pd = self.strip_quotes(pd)
        ie = clean_pd.find_all("img", attrs={"alt": ""})
        return ie

    def remove_amps(self, sac_string):
        cleaned = re.sub("amp;", "", str(sac_string))
        return cleaned

    def dupe_check(self, id, db_list:[]) -> bool:
        return id in set(db_list)

    def get_last_subforum_page(self, page):
        soup = BeautifulSoup(page, "lxml")
        div_pages = soup.find("div", attrs={"class": "pages top"})
        dp_ck = None if not div_pages else div_pages.option
        page_list = None if not dp_ck else [int(x.text) for x in div_pages.find_all("option")]
        return 1 if not page_list else page_list[-1]

    def get_current_subforum_page(self, page):
        soup = BeautifulSoup(page, "lxml")
        current_page = soup.find("option", attrs={"selected":"selected"})
        current_page = 1 if not current_page else int(current_page.text)
        return current_page

    def get_subforums(self, fid, subs) -> pd.DataFrame():
        if not subs:
            return None
        else:
            subs_data = []
            current_subs = self.sql.get_table_cols("subforums", ["sub_id"])
            for i,j in enumerate(subs):
                if self.dupe_check(subs[i], current_subs):
                    logger.info(f"{fid}: already exists, skipping")
                    continue
                else:
                    d_subforums = {
                        "forum_id": int(fid),
                        "sub_id": int(subs[i])
                    }
                    subs_data.append(d_subforums)
                    logger.debug(f"{fid}: Row prepared for goons.subforums | {d_subforums}")
            subforums = pd.DataFrame(subs_data)
            return subforums

    def get_new_threads(self, page) -> {}:
        soup = BeautifulSoup(page, "lxml")
        forum_list = soup.find("table", attrs={"id":"forum"})
        page_tids_str = re.findall(r.re_sub_tid, str(forum_list))
        page_tids = set([eval(i) for i in page_tids_str])
        db_tids = set(self.sql.get_table_cols("threads", ["thread_id"])["thread_id"].tolist())
        return page_tids.difference(db_tids)

    def get_thread_links(self, p_div):
        links_thread = pd.unique(re.findall(r.re_th, str(p_div)))
        lt_cleaned = [self.remove_amps(lt) for lt in links_thread]
        return lt_cleaned

    def get_saclopedia_links(self, p_div):
        links_saclopedia = pd.unique(re.findall(r.re_sp, str(p_div)))
        lsp_clean = [self.remove_amps(lsp) for lsp in links_saclopedia]
        lsp_parsed = [None if not links_saclopedia.all() else parse_qs(urlparse(lsp).query) for lsp in lsp_clean]
        return lsp_parsed

    def get_frontpage_links(self, p_div):
        links_frontpage = pd.unique(re.findall(r.re_fr, str(p_div)))
        lfp_parsed = None if not links_frontpage.all() else [urlparse(lfp[0])[2] for lfp in links_frontpage]
        rfp_matches = None if not lfp_parsed else [re.search(r.re_fpp, str(lfp)) for lfp in lfp_parsed]
        return rfp_matches

    def get_profile_links(self, p_div) -> []:
        uc = UserCheck()
        links_profile = pd.unique(re.findall(r.re_prof, str(p_div)))
        lp_cleaned = [self.remove_amps(lp) for lp in links_profile]
        lp_unicoded = [re.sub('%20', ' ', lp) for lp in lp_cleaned]
        lpr_userids = []
        for lp in lp_unicoded:
            uid_ck = re.search(r.re_userid, str(lp))
            uid = None if not uid_ck else uid_ck.group(1)
            un_ck = re.search(r.re_username, str(lp))
            username = None if not un_ck else un_ck.group(1)
            if uid and not username:
                lpr_userids.append(int(uid))
            elif username and not uid:
                u_id = uc.get_uid_from_username(username)
                lpr_userids.append(int(u_id))
            else:
                lpr_userids.append(666)
        # lpr_userids = [None if not re.search(r.re_userid, str(lpr)) else re.search(r.re_userid, str(lpr)).group(1) for lpr in lp_unicoded]
        return lpr_userids

    def get_emotes(self, body):
        emoticons = re.findall("forumsystem/emoticons/emot-([^\.]+)\.\w{3,4}\"", str(body))
        cnt_emotes = Counter([str(emote) for emote in emoticons])
        return cnt_emotes

    def get_quotes(self, p_div):
        quotepairs = []
        quotes = p_div.find_all("div", attrs={"class": "bbc-block"})
        if not quotes:
            pass
        else:
            for q in quotes:
                qt = None if not q.a else q.a.text
                ck = None if not qt else re.search("^(.+)\sposted:", qt)    # check if there's a quoted user, don't care enough to account for exceptions rn
                un = "" if not ck else ck.group(1)
                cl = None if not q.a else re.search("postid=(\d+)", q.a["href"])
                pi = 666 if not cl else (cl).group(1)
                quotepairs.append((un, pi))
        return None if not quotes else quotepairs

    def parse_post_quotes(self, post_div, pid, tid, username) -> None:
        quotes = self.get_quotes(post_div)
        if not quotes:
            logger.debug(f"{pid}: No quotes found")
        else:
            pq_list = []
            for user,qtid in quotes:
                post_quotes = {
                    "post_id" : int(pid),
                    "quoted_user" : user,
                    "quoted_post" : int(qtid),
                    "is_external_quote" : bool(tid==qtid),
                    "is_self_quote" : bool(username==user),
                    "is_emptyquote" : bool(not self.emptyquote_check(str(post_div)))
                    }
                pq_list.append(post_quotes)
                dataframe = pd.DataFrame([post_quotes])
                if not dataframe.empty:
                    self.generate_index_and_upsert(pid=pid, dataframe=dataframe,tablename="post_quotes")
        return None

    def parse_leper_quotes(self, body, l_index) -> []:
        quotes = self.get_quotes(body)
        if not quotes:
            logger.debug(f"{l_index}: No quotes found")
            return None
        else:
            lq_list = []
            for user,qtid in quotes:
                post_quotes = {
                    "l_index" : str(l_index),
                    "quoted_user" : user,
                    "quoted_post" : int(qtid)
                    }
                lq_list.append(post_quotes)
                logger.debug(f"{l_index}: Row prepared for leper_quotes | {post_quotes}")
            logger.debug(f"{l_index}: leper_quotes data ready for Dataframe")
            return lq_list

    def generate_index_and_upsert(self, pid, dataframe, tablename:str):
        success = False
        tries = 10
        while success is False and tries > 0:
            try:
                dataframe["p_index"] = uuid.uuid4()
                df_reindexed = dataframe.set_index("p_index")
                self.sql.upsert_df(df_reindexed, tablename)
                success = True
                logger.debug(f"{pid} | Row inserted into {tablename}")
                print(f"{pid} | Row inserted into {tablename}")
            except Exception as e:
                logger.warn(e)
                tries -= 1
                pass
        if tries == 0:
            exit()
        return

    def parse_post_emotes(self, post_div, pid) -> None:
        post_emoticons = self.get_emotes(post_div)
        if not post_emoticons:
            logger.debug(f"{pid}: No emotes found")
            return
        else:
            for emote in post_emoticons:
                post_emotes = {
                    "post_id" : int(pid),
                    "emote" : emote,
                    "count" : post_emoticons[emote]
                    }
                logger.debug(f"{pid}: Row prepared for post_emotes | {post_emotes}")

                self.sql.upsert_df(pd.DataFrame([post_emotes]).set_index(["post_id", "emote"]), "post_emotes")
                logger.debug(f"{pid} | Row inserted into post_emotes")
            return

    def parse_leper_emotes(self, body, l_index) -> []:
        leper_emoticons = self.get_emotes(body)
        if not leper_emoticons:
            logger.debug(f"{l_index}: No emotes found")
            return None
        else:
            pe_list = []
            for emote in leper_emoticons:
                leper_emotes = {
                    "l_index" : str(l_index),
                    "emote" : emote,
                    "count" : leper_emoticons[emote]
                    }
                pe_list.append(leper_emotes)
                logger.debug(f"{l_index}: Row prepared for leper_emotes | {leper_emotes}")
            logger.debug(f"{l_index}: leper_emotes data ready for Dataframe")
            return pe_list

    def parse_embedded_images(self, pd_noquotes, pid) -> []:
        post_embed_images = pd_noquotes.find_all("img", attrs={"alt": ""})
        if not post_embed_images:
            logger.debug(f"{pid}: No embedded images found")
            return None
        else:
            pei_list = []
            re_ei = re.findall(r.re_embed_images, str(post_embed_images))
            is_l_index = len(str(pid)) == 25
            for pei in re_ei:
                if is_l_index:
                    d_embedded_images = {
                        "l_index" : str(pid),
                        "image_domain" : None if not pei[0] else pei[0],
                        "image_filename" : None if not pei[1] else pei[1],
                        "image_fileformat" : None if not pei[2] else pei[2]
                        }
                    logger.debug(f"{pid}: Row prepared for leper_embedded_images | {d_embedded_images}")
                else:
                    d_embedded_images = {
                        "post_id" : int(pid),
                        "image_domain" : None if not pei[0] else pei[0],
                        "image_filename" : None if not pei[1] else pei[1],
                        "image_fileformat" : None if not pei[2] else pei[2]
                        }
                    dataframe = pd.DataFrame([d_embedded_images])
                    if not dataframe.empty:
                        self.generate_index_and_upsert(pid=pid, dataframe=dataframe,tablename="post_embed_images")
                
                # TODO: Make similar changes to the leper parsers, maybe split them out somehow
                pei_list.append(d_embedded_images)
            return pei_list

    def parse_embedded_videos(self, pd_noquotes, pid) -> []:
        post_embed_videos = pd.unique(re.findall(r.re_embed_videos, str(pd_noquotes)))
        if not post_embed_videos.size > 0:
            logger.debug(f"{pid}: No embedded videos found")
            return None
        else:
            pev_list = []
            is_l_index = len(str(pid)) == 25
            for domain,filename in post_embed_videos:
                if is_l_index:
                    d_embedded_video = {
                        "l_index" : str(pid),
                        "video_domain" : domain,
                        "video_filename" : filename
                        }
                    pev_list.append(d_embedded_video)
                    logger.debug(f"{pid}: Row prepared for leper_embedded_videos | {d_embedded_video}")
                else:
                    d_embedded_video = {
                        "post_id" : int(pid),
                        "video_domain" : domain,
                        "video_filename" : filename,
                        }
                    dataframe = pd.DataFrame([d_embedded_video])
                    if not dataframe.empty:
                        self.generate_index_and_upsert(pid=pid, dataframe=dataframe,tablename="post_embed_videos")

                    logger.debug(f"{pid}: Row prepared for post_embed_videos | {d_embedded_video}")
            if is_l_index:
                logger.debug(f"{pid}: leper_embedded_videos data ready for Dataframe")
            else:
                logger.debug(f"{pid}: post_embed_videos data ready for Dataframe")
            return pev_list

    def parse_embedded_youtubes(self, pd_noquotes, pid) -> []:
        post_youtubes = pd.unique(re.findall(r.re_youtube, str(pd_noquotes)))
        if not post_youtubes.size > 0:
            logger.debug(f"{pid}: No youtubes found")
            return None
        else:
            pyt_list = []
            is_l_index = len(str(pid)) == 25
            for x,video in post_youtubes:
                if is_l_index:
                    d_embedded_youtubes = {
                        "l_index" : str(pid),
                        "video" : video
                        }
                    pyt_list.append(d_embedded_youtubes)
                    logger.debug(f"{pid}: Row prepared for post_embed_youtubes | {d_embedded_youtubes}")
                else:
                    d_embedded_youtubes = {
                        "post_id" : int(pid),
                        "video" : video,
                        }
                    dataframe = pd.DataFrame([d_embedded_youtubes])
                    if not dataframe.empty:
                        self.generate_index_and_upsert(pid=pid, dataframe=dataframe,tablename="post_embed_youtubes")

            # TODO: Fix leper, return None
            return pyt_list

    def parse_embedded_twitters(self, pd_noquotes, pid) -> []:
        post_twitters = pd.unique(re.findall(r.re_twitter, str(pd_noquotes)))
        if not post_twitters.size > 0:
            logger.debug(f"{pid}: No twitters found")
            return None
        else:
            ptw_list = []
            is_l_index = len(str(pid)) == 25
            for x,account,post in post_twitters:
                if is_l_index:
                    d_embedded_twitter = {
                        "l_index" : str(pid),
                        "twitter_account" : account,
                        "twitter_post" : post
                        }
                    ptw_list.append(d_embedded_twitter)
                    logger.debug(f"{pid}: Row prepared for leper_embedded_twitters | {d_embedded_twitter}")
                else:
                    d_embedded_twitter = {
                        "post_id" : int(pid),
                        "twitter_account" : account,
                        "twitter_post" : post,
                        }
                    dataframe = pd.DataFrame([d_embedded_twitter])
                    if not dataframe.empty:
                        self.generate_index_and_upsert(pid=pid, dataframe=dataframe,tablename="post_embed_twitters")

        # TODO: Fix leper, return none
        return ptw_list

    def parse_links_logout(self, pd_noquotes, pid) -> []:
        post_links_logout = pd_noquotes.find_all("a", attrs={"href": re.compile("htt.+account\.php\?action=logout.+")})
        if not post_links_logout:
            logger.debug(f"{pid}: No logout links found")
            return None
        else:
            ll_list = []
            for ll in post_links_logout:
                d_links_logout = {
                    "post_id" : int(pid),
                    "link_text" : ll.string
                    }
                logger.debug(f"{pid}: Row prepared for post_links_logout | {d_links_logout}")

                dataframe = pd.DataFrame([d_links_logout])
                if not dataframe.empty:
                    self.generate_index_and_upsert(pid=pid, dataframe=dataframe,tablename="post_links_logout")
            return

    def parse_links_leper(self, pd_noquotes, pid) -> []:
        post_links_leper = pd.unique(re.findall(r.re_bl, str(pd_noquotes)))
        if not post_links_leper.size > 0:
            logger.debug(f"{pid}: No banlist links found")
            return None
        else:
            for link in post_links_leper:
                user = re.search(r.re_bl_user, link)
                post = re.search(r.re_bl_post, link)
                approver = re.search(r.re_bl_appr, link)
                d_links_leper = {
                    "post_id" : int(pid),
                    "punished_user" : None if not user else user.group(1),
                    "punished_post" : None if not post else int(post.group(1)),
                    "approver" : None if not approver or approver.group(1)=='' else int(approver.group(1))
                    }
                logger.debug(f"{pid}: Row prepared for post_links_leper | {d_links_leper}")
                dataframe = pd.DataFrame([d_links_leper])
                if not dataframe.empty:
                    self.generate_index_and_upsert(pid=pid, dataframe=dataframe,tablename="post_links_leper")
            return

    def parse_links_thread(self, pd_noquotes, pid) -> []:
        post_links_thread = self.get_thread_links(pd_noquotes)
        if not post_links_thread:
            logger.debug(f"{pid}: No thread links found")
            return None
        else:
            lt_list = []
            is_l_index = len(str(pid)) == 25
            for ltp in post_links_thread:

                lt_parsed = parse_qs(urlparse(ltp).query)
                p_page = None if not lt_parsed.get('pagenumber') else re.search("\d+", lt_parsed['pagenumber'][0])
                uid_field = None if not lt_parsed.get('userid') else lt_parsed['userid'][0]
                uid_cleaned = None if not uid_field else re.search("\d+",uid_field).group()

                threadid = None if not lt_parsed.get('threadid') else int(re.search("\d+", lt_parsed['threadid'][0]).group())
                page_no = None if not p_page else int(p_page.group(0))
                uid =  None if not uid_cleaned else int(uid_cleaned)
                pid_chk = re.search("#post(\d+)", str(ltp))
                postid = None if not pid_chk else int((pid_chk).group(1))

                if is_l_index:
                    d_links_thread = {
                        "l_index" : str(pid),
                        "threadid" : threadid,
                        "pageno" : page_no,
                        "userid" : uid,
                        "postid" : postid
                        }
                    lt_list.append(d_links_thread)
                    logger.debug(f"{pid}: Row prepared for leper_links_thread | {d_links_thread}")
                else:
                    d_links_thread = {
                        "post_id" : int(pid),
                        "threadid" : threadid,
                        "pageno" : page_no,
                        "userid" : None if not uid else int(uid),
                        "postid" : postid,
                        }
                    dataframe = pd.DataFrame([d_links_thread])
                    if not dataframe.empty:
                        self.generate_index_and_upsert(pid=pid, dataframe=dataframe,tablename="post_links_thread")
        return lt_list


    # TODO: Better nulling logic for all the little post tables

    def parse_links_saclopedia(self, pd_noquotes, pid) -> []:
        post_links_saclopedia = self.get_saclopedia_links(pd_noquotes)
        if not post_links_saclopedia:
            logger.debug(f"{pid}: No SAclopedia links found")
            return None
        else:
            ls_list = []
            for lsp in post_links_saclopedia:
                act_check = None if not lsp else lsp.get('act')
                l_check = None if not lsp else lsp.get('l')
                topicid_check = None if not lsp else lsp.get('topicid')
                d_links_saclopedia = {
                    "post_id" : int(pid),
                    "act" : None if not act_check else lsp['act'],           # tree level?
                    "l" : None if not l_check else lsp['l'],                 # some kind of page index?
                    "topicid" : None if not topicid_check else re.search("\d+", str(lsp['topicid'])).group()
                    }
                dataframe = pd.DataFrame([d_links_saclopedia])
                if not dataframe.empty:
                    self.generate_index_and_upsert(pid=pid, dataframe=dataframe,tablename="post_links_saclopedia")
            return ls_list

    def parse_links_frontpage(self, pd_noquotes, pid) -> []:
        post_links_frontpage = self.get_frontpage_links(pd_noquotes)
        if not post_links_frontpage:
            logger.debug(f"{pid}: No frontpage links found")
            return None
        else:
            for match in post_links_frontpage:
                r_page = None if not match else match.group(3)
                page_no = None if not r_page else int(r_page)
                d_links_frontpage = {
                    "post_id" : int(pid),
                    "feature" : None if not match else match.group(1),
                    "title" : None if not match else match.group(2),
                    "page" : None if not match else page_no
                    }
                dataframe = pd.DataFrame([d_links_frontpage])
                if not dataframe.empty:
                    self.generate_index_and_upsert(pid=pid, dataframe=dataframe,tablename="post_links_frontpage")
            return

    def parse_links_profile(self, pd_noquotes, pid) -> []:
        post_links_profile = self.get_profile_links(pd_noquotes)
        if not post_links_profile:
            logger.debug(f"{pid}: No profile links found")
            return None
        else:
            pr = []
            uc = UserCheck()
            is_l_index = len(str(pid)) == 25
            for lpr in post_links_profile:
                if is_l_index:
                    d_links_profile = {
                        "l_index" : str(pid),
                        "userid" : 666 if not post_links_profile else int(lpr.group(1)),
                        "user_exists" : None if not post_links_profile else uc.check_if_user_real(post_links_profile)
                        }
                    pr.append(d_links_profile)
                    logger.debug(f"{pid}: Row prepared for leper_links_profile | {d_links_profile}")
                else:
                    d_links_profile = {
                        "post_id" : pid,
                        "userid" : 666 if not lpr else int(lpr),
                        "user_exists" : None if not lpr else uc.check_if_user_real(post_links_profile),
                        }
                    dataframe = pd.DataFrame([d_links_profile])
                    if not dataframe.empty:
                        self.generate_index_and_upsert(pid=pid, dataframe=dataframe,tablename="post_links_profile")
            return pr

    def parse_forumdisplay(self, page):
        soup = BeautifulSoup(page, 'lxml')
        df_forums = pd.DataFrame()
        df_subforums = pd.DataFrame()

        subs_soup = soup.find("table", attrs={"id":"subforums"})
        sub_ids = re.findall("forumid=(\d+)", str(subs_soup))
        forum_id = int(re.search("<div class=\"breadcrumbs\"><span.+forumid=(\d+).+</span>", str(soup)).group(1))

        current_forums = self.sql.get_table_cols("forums", ["forum_id"])
        if self.dupe_check(forum_id, current_forums):
            logger.info(f"{forum_id}: exists in database, skipping")
            this_forums = {}
        else:
            this_forums = {
                "forum_id": forum_id,
                "forum_name": str(soup.find("a", attrs={"class":"bclast"}).text)
                }
            df_forums = pd.DataFrame([this_forums])
            logger.debug(f"{forum_id}: Row prepared for goons.forums | {this_forums}")
        forumdisplay_data = {}
        df_subforums = self.get_subforums(forum_id, sub_ids)
        forumdisplay_data = {
            "forums" : pd.DataFrame() if df_forums.empty else df_forums,
            "subforums" : pd.DataFrame() if not sub_ids else df_subforums
        }
        return forumdisplay_data

    def parse_posts(self, page):
        is_error = self.error_check(page)
        if is_error == "general":
            logger.info("THREAD NOT FOUND IN LIVE FORUMS")
            print("THREAD NOT FOUND IN LIVE FORUMS")
            post_data = pd.DataFrame()
        elif is_error == "gateway" or is_error == "banned":
            logger.info(f"!!!!FATAL!!!! | Exiting. Reason: {is_error}")
            print(f"!!!!FATAL!!!! | Exiting. Reason: {is_error}")
            exit()

        else:
            html_close_tag_count = len(re.findall("</html", page))
            while html_close_tag_count > 1:
                page = re.sub("</html", "", page)
                html_close_tag_count = len(re.findall("</html", page))

            input_clean = re.sub("<!--\sgoogle_ad_section_start\s-->\s|<!--\sgoogle_ad_section_end\s-->\s", "", str(page))
            soup = BeautifulSoup(input_clean, 'lxml')
            post_soups = soup.find_all('table', attrs={"class": "post"})

            page_post_metadata = []

            thread_id_and_title = soup.find("a", attrs={"href": re.compile(".+threadid.+")})
            thread_id = int((re.search("\d+", str(thread_id_and_title))).group())
            # thread_title = thread_id_and_title.text
            page_check = soup.find("option", attrs={"selected":"selected"})
            page_number = 1 if not page_check else int(page_check.text)
            logger.info(f"Thread {thread_id} p{page_number}: Parsing posts...")

            post_list = []
            for this_post in post_soups:
                post_list.append(this_post)

            per_40_post_index = 0   # I got so far into this before deciding I wanted to track this!
                                    # I'm in too deep! I don't care that it's not """pythonic"""!
                                    # I'm not rewriting this with enumerate()! ... not yet anyway
            for post in post_list:
                post = BeautifulSoup(str(post), 'lxml')

                # Different views of the post text
                post_div = post.find("td", attrs={"class": "postbody"})
                pd_noquotes = self.strip_quotes(post_div)
                post_puretext = self.strip_non_text(str(post_div))
                purer_text = re.sub(r.re_prr_txt, " ", post_puretext)
                post_wordlist = set((purer_text.split()))

                username = post.dt.text
                post_id = int((re.search("\d+", post.table["id"])).group())
                userid = int((re.search("userid-(\d+)", str(post))).group(1))

                logger.info(f"Parsing post {post_id}...")

                edited_soup = post.find("p", attrs={"editedby"})
                edited_field = None if not edited_soup else post.find("p", attrs={"editedby"}).get_text()
                is_empty_string = bool(edited_field=="" or not edited_field)
                is_newline = bool(edited_field=="\n" or not edited_field)

                if is_empty_string or is_newline:
                    is_edited = False
                else:
                    is_edited = True

                # TODO: NONE IF NOT
                edit_dt_str = None if not is_edited else (re.search("\d{2}:\d{2}\s\w{2}\s\w{3}\s\d{1,2},\s\d{4}", str(edited_soup))).group()
                edit_tm = None if not is_edited else datetime.strptime((re.search("\d{2}:\d{2}", edit_dt_str)).group(), "%H:%M")
                edit_dt = None if not is_edited else datetime.strptime((re.search("\w{3}\s\d{1,2},\s\d{4}", edit_dt_str)).group(), "%b %d, %Y")
                editor = None if not is_edited else re.search("span>(.+)\shas|(.+)\sfucked", str(edited_soup))

                datetime_parsed = post.find("td", attrs={"class": "postdate"}).text
                datetime_string = (re.search(r.re_post_datetime, datetime_parsed)).group()

                d_post_metadata = {
                    "post_id" : int(post_id),
                    "thread_id" : int(thread_id),
                    "userid" : int(userid),
                    "post_datetime" : datetime.strptime(datetime_string, '%b %d, %Y %H:%M'),
                    "page" : page_number,
                    "per_40_index" : per_40_post_index,
                    "cnt_words" : len(post_wordlist),
                    "edited" : False if not is_edited else True,
                    "edited_datetime" : None if not is_edited else datetime.combine(edit_dt, edit_tm.time()),
                    "edited_by_other" : None if not is_edited else bool(username == editor.group(1))
                }
                logger.debug(f"{post_id}: Row prepared for goons.post_meta | {d_post_metadata}")
                page_post_metadata.append(d_post_metadata)

                # Parse and upload any other interesting information about the post, individually
                self.parse_post_quotes(post_div, post_id, thread_id, username)
                self.parse_post_emotes(pd_noquotes, post_id)
                self.parse_embedded_images(pd_noquotes, post_id)
                self.parse_embedded_videos(pd_noquotes, post_id)
                self.parse_embedded_youtubes(pd_noquotes, post_id)
                self.parse_embedded_twitters(pd_noquotes, post_id)
                self.parse_links_logout(pd_noquotes, post_id)
                self.parse_links_leper(pd_noquotes, post_id)
                self.parse_links_thread(pd_noquotes, post_id)
                self.parse_links_saclopedia(pd_noquotes, post_id)
                self.parse_links_frontpage(pd_noquotes, post_id)
                self.parse_links_profile(pd_noquotes, post_id)

                per_40_post_index += 1      # I'm not happy about it either!!

            post_data = pd.DataFrame() if len(page_post_metadata)==0 else pd.DataFrame(page_post_metadata).set_index("post_id")
            logger.info(f"Thread {thread_id} p{page_number}: PARSE COMPLETE. Upserting post metadata...")
        return post_data


    def parse_profile(self, page):
        # If there are more than one </html> closing tag for some reason,
        html_close_tag_count = len(re.findall("</html", page))
        while html_close_tag_count > 1:
            page = re.sub("</html", "", page)
            html_close_tag_count = len(re.findall("</html", page))

        soup = BeautifulSoup(page, 'lxml')
        users_data = {
            "userid" : int,
            "username": str,
            "avatar_url" : str,
            "regdate" : datetime,
            "account_type" : str,
            "postcount" : int,
            "post_rate" : float,
            "last_post" : datetime,
            "has_userpic" : bool
        }

        av_tag = soup.find("dd", attrs={"class":"title"})
        r_av = re.search(r.re_av, str(av_tag))
        av_url = None if not r_av else str(r_av.group(1))
        gangtags = [gt for gt in re.findall(r.re_gt, str(av_tag))[1:]]
        userid = int(re.search(r.re_puid, str(soup)).group(1))
        never = re.search("<dd>Never", page)

        logger.info(f"{userid}: Parsing profile...")
        users_data = {
            "userid" : [int(userid)],
            "username" : [soup.dt.text],
            "avatar_url" : [None if not av_url else av_url],
            "regdate" : [datetime.strptime(soup.find("dd", attrs={"class": "registered"}).text, "%b %d, %Y")],
            "account_type" : [str(soup.find("dt", attrs={"title":re.compile(".+")})["title"])],
            "postcount" : [int(re.search(r.re_pc, str(soup)).group(1))],
            "post_rate" : [float(re.search(r.re_pr, str(soup)).group(1))],
            "last_post" : ["1900-01-01 00:00:00" if never else datetime.strptime(re.search(r.re_lp, str(soup)).group(1), "%b %d, %Y %H:%M")],
            "has_userpic" : [bool(soup.find("div", attrs={"class":"userpic"}))]
        }
        logger.debug(f"{userid}: Row prepared for goons.users | {users_data}")

        if len(gangtags)==0:
            logger.debug(f"{userid}: No gangtags found")
        else:
            for tag in gangtags:
                users_gangtags = {
                    "userid" : int(userid),
                    "gangtag" : str(tag)
                }
                dataframe = pd.DataFrame([users_gangtags])
                self.generate_index_and_upsert(pid=userid, dataframe=dataframe,tablename="users_gangtags")

        df_users = pd.DataFrame.from_dict(users_data).set_index("userid")
        logger.info(f"{userid}: Profile parsing COMPLETE")
        return df_users

    def parse_leper(self, page):
        soup = BeautifulSoup(page, 'lxml')
        entry_soups = soup.find_all('tr', attrs={'data-postid': re.compile(r.re_post_id)})

        page_leper = []
        page_leper_quotes = []
        page_leper_emotes = []
        page_leper_embedded_images = []
        page_leper_embedded_videos = []
        page_leper_embedded_youtubes = []
        page_leper_embedded_twitters = []
        page_leper_links_thread = []
        page_leper_links_profile = []

        entries = []
        for this_tr in entry_soups:
            entries.append(this_tr)

        for i,tr in enumerate(entries):
            tds = tr.find_all("td")
            body = tds[3]
            body_noquotes = self.strip_quotes(body)

            post_id = tr['data-postid']

            re_punish_desc = re.search(r.re_punish_length, str(entries[i].contents[3].contents))
            dt_str = "".join(tr.contents[1].contents)
            punish_dt = datetime.strptime(dt_str, '%m/%d/%y %I:%M%p')
            l_index = Randoms.generate_p_index(random.randint(70000,999999))[:25]

            logger.info(f"{l_index}: Parsing entry...")

            d_leper = {
                "postid": int(post_id),
                "punish_type": "".join(tr.contents[0].a.contents),
                "punish_datetime": punish_dt,
                "userid": int((re.search(r.re_userid_leper, str(tr.contents[2].a))).group()),
                "punish_length": None if not re_punish_desc else re_punish_desc.group(),
                "requestor_id": int((re.search(r.re_userid_leper, str(tr.contents[4].a))).group()),
                "approver_id": int((re.search(r.re_userid_leper, str(tr.contents[5].a))).group()),
                "l_index": l_index
            }
            logger.debug(f"{l_index}: Row prepared for goons.leper | {d_leper}")
            d_leper_quotes = self.parse_leper_quotes(body, l_index)
            d_leper_emotes = self.parse_leper_emotes(body, l_index)
            d_leper_embedded_images = self.parse_embedded_images(body_noquotes, l_index)
            d_leper_embedded_videos = self.parse_embedded_videos(body_noquotes, l_index)
            d_leper_embedded_youtubes = self.parse_embedded_youtubes(body_noquotes, l_index)
            d_leper_embedded_twitters = self.parse_embedded_twitters(body_noquotes, l_index)
            d_leper_links_thread = self.parse_links_thread(body_noquotes, l_index)
            d_leper_links_profile = self.parse_links_profile(body_noquotes, l_index)

            page_leper.append(None if not d_leper else d_leper)
            page_leper_quotes.extend([] if not d_leper_quotes else d_leper_quotes)
            page_leper_emotes.extend([] if not d_leper_emotes else d_leper_emotes)
            page_leper_embedded_images.extend([] if not d_leper_embedded_images else d_leper_embedded_images)
            page_leper_embedded_videos.extend([] if not d_leper_embedded_videos else d_leper_embedded_videos)
            page_leper_embedded_youtubes.extend([] if not d_leper_embedded_youtubes else d_leper_embedded_youtubes)
            page_leper_embedded_twitters.extend([] if not d_leper_embedded_twitters else d_leper_embedded_twitters)
            page_leper_links_thread.extend([] if not d_leper_links_thread else d_leper_links_thread)
            page_leper_links_profile.extend([] if not d_leper_links_profile else d_leper_links_profile)

        leper_data = {}
        df_leper = pd.DataFrame(page_leper)
        df_leper_quotes = pd.DataFrame(None if len(page_leper_quotes) == 0 else page_leper_quotes)
        df_leper_emotes = pd.DataFrame(None if len(page_leper_emotes) == 0 else page_leper_emotes)
        df_leper_embedded_images = pd.DataFrame(None if len(page_leper_embedded_images) == 0 else page_leper_embedded_images)
        df_leper_embedded_videos = pd.DataFrame(None if len(page_leper_embedded_videos) == 0 else page_leper_embedded_videos)
        df_leper_embedded_youtubes = pd.DataFrame(None if len(page_leper_embedded_youtubes) == 0 else page_leper_embedded_youtubes)
        df_leper_embedded_twitters = pd.DataFrame(None if len(page_leper_embedded_twitters) == 0 else page_leper_embedded_twitters)
        df_leper_links_thread = pd.DataFrame(None if len(page_leper_links_thread) == 0 else page_leper_links_thread)
        df_leper_links_profile = pd.DataFrame(None if len(page_leper_links_profile) == 0 else page_leper_links_profile)

        leper_data = {
            "leper": df_leper.set_index("l_index"),
            "leper_quotes": df_leper_quotes if df_leper_quotes.empty else df_leper_quotes.set_index(["l_index", "quoted_post"]),
            "leper_emotes": df_leper_emotes if df_leper_emotes.empty else df_leper_emotes.set_index(["l_index", "emote"]),
            "leper_embedded_images": df_leper_embedded_images if df_leper_embedded_images.empty else df_leper_embedded_images.set_index(["l_index", "image_filename"]),
            "leper_embedded_videos": df_leper_embedded_videos if df_leper_embedded_videos.empty else df_leper_embedded_videos.set_index(["l_index", "video_filename"]),
            "leper_embedded_youtubes": df_leper_embedded_youtubes if df_leper_embedded_youtubes.empty else df_leper_embedded_youtubes.set_index(["l_index", "video"]),
            "leper_embedded_twitters": df_leper_embedded_twitters if df_leper_embedded_twitters.empty else df_leper_embedded_twitters.set_index(["l_index", "twitter_post"]),
            "leper_links_thread": df_leper_links_thread if df_leper_links_thread.empty else df_leper_links_thread.set_index(["l_index", "threadid"]),
            "leper_links_profile": df_leper_links_profile if df_leper_links_profile.empty else df_leper_links_profile.set_index(["l_index", "userid"])
        }
        logger.debug("Leper data for this page ready for Dataframe")
        return leper_data

    def parse_subforum(self, page):
        is_error = self.error_check(page)
        if is_error == "general":
            logger.info("General error. Skipping...")
            print("General error. Skipping...")
            thread_data = {}
        elif is_error == "gateway" or is_error == "banned":
            logger.info(f"!!!!FATAL!!!! | Exiting. Reason: {is_error}")
            print(f"!!!!FATAL!!!! | Exiting. Reason: {is_error}")
            exit()
        else:
            soup = BeautifulSoup(page, 'lxml')
            re_thread_class = re.compile("thread.*")
            this_thread_list = soup.find_all('tr', attrs={"class": re_thread_class})
            current_db_threads = set(self.sql.get_table_cols("threads", cols=["thread_id"])["thread_id"])

            d_thread_ids = []
            d_subforum_ids = []
            d_thread_titles = []
            d_thread_icons = []
            d_thread_authors = []
            d_cnt_replies = []
            d_cnt_views = []
            d_ratings = []
            d_votes = []
            d_killed_by = []
            d_killed_by_date = []
            d_is_closed = []
            df_thread_data = pd.DataFrame()

            for thread in this_thread_list:
                if re.search("announcement\.php", str(thread)):
                    continue
                post_icon = re.search("posticon=\d+", str(thread.find("td", attrs={"class": "icon"})))
                this_forumid_href = (re.search("forumid=\d+", str(soup))).group()
                this_forumid = (re.search("\d+", this_forumid_href)).group()
                this_posticon_href = None if not post_icon else post_icon.group()
                this_author_href = (re.search("userid=\d+", str(thread.find("td", attrs={"class": "author"})))).group()
                this_rating_class = str(thread.find("td", attrs={"class": "rating"}))
                this_rating_check = re.search("\d{1}\.\d{2}", this_rating_class)
                this_rating = float((this_rating_check).group()) if this_rating_check else None
                this_votes_check = re.search("(\d+)\svotes", this_rating_class)
                this_votes = (this_votes_check).group(1) if this_votes_check else None
                this_date_string = thread.find("td", attrs={"class": "lastpost"}).find("div").string

                thread_id = re.search("thread(\d+)", str(thread)).group(1)
                logger.info(f"{thread_id}: parsing thread index...")

                if thread_id in current_db_threads:
                    logger.info(f"{thread_id}: already exists, skipping")
                    continue
                else:
                    subforum_id = int(this_forumid)
                    thread_title = str(thread.find("a", attrs={"class": "thread_title"}).string)
                    thread_icon = None if not this_posticon_href else str((re.search("\d+", this_posticon_href)).group())
                    thread_author = str((re.search("\d+", this_author_href)).group())
                    cnt_replies = int(thread.find("td", attrs={"class": "replies"}).string)
                    cnt_views = int(thread.find("td", attrs={"class": "views"}).string)
                    rating = float(this_rating) if this_rating else None
                    votes = int(this_votes) if this_votes else None
                    killed_by = str(thread.find("td", attrs={"class": "lastpost"}).find("a").string)
                    killed_by_date = datetime.strptime(this_date_string, "%H:%M %b %d, %Y")
                    closed_status = False if not re.search("thread closed", str(thread)) else True

                    d_thread_ids.append(int(thread_id))
                    d_subforum_ids.append(subforum_id)
                    d_thread_titles.append(thread_title)
                    d_thread_icons.append(thread_icon)
                    d_thread_authors.append(thread_author)
                    d_cnt_replies.append(cnt_replies)
                    d_cnt_views.append(cnt_views)
                    d_ratings.append(rating)
                    d_votes.append(votes)
                    d_killed_by.append(killed_by)
                    d_killed_by_date.append(killed_by_date)
                    d_is_closed.append(closed_status)
                    logger.debug(f"{thread_id}: Row prepared for goons.threads | no dict to display because I don't wanna refactor yet")

            thread_data = {
                "thread_id": d_thread_ids,
                "subforum_id": d_subforum_ids,
                "thread_title": d_thread_titles,
                "thread_icon": d_thread_icons,
                "thread_author": d_thread_authors,
                "cnt_replies": d_cnt_replies,
                "cnt_views": d_cnt_views,
                "rating": d_ratings,
                "votes": d_votes,
                "killed_by": d_killed_by,
                "killed_by_date": d_killed_by_date,
                "is_closed": d_is_closed
            }
            df_thread_data = pd.DataFrame(thread_data).set_index("thread_id")
        return df_thread_data
