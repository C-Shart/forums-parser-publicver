import config
import sessions
import re

sa_user = config.sa_user
sa_pass = config.sa_pass
sa_uid = config.sa_uid
sa_bbpwd = config.sa_bbpwd
sa_aduserid = config.sa_aduserid

session = sessions.begin_session()

class PageGetters:

    # ARCHIVES
    def enter_archives(self, fid, year:int, sess=session):
        payload = {
            'forumid' : fid,
            'ac_month' : None,
            'bday_day': None,
            'ac_year': str(year),
            'set': 'GO'
            }
        cookie_dict = sess.cookies.get_dict()
        sess_cookie = cookie_dict['__cf_bm']
        sess.headers.update({
            'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8', 
            'Accept-Encoding' : 'gzip, deflate, br', 
            'Accept-Language' : 'en-US,en;q=0.5', 
            'Connection' : 'keep-alive',
            'Cache-Control': 'max-age=0', 
            'Cookie' : f'fdfilt=0000{year}; bbuserid={sa_uid}; bbpassword={sa_bbpwd}; aduserid={sa_aduserid}; __cf_bm={sess_cookie}',
            'Host' : 'forums.somethingawful.com',
            'Sec-Fetch-Dest' : 'document', 
            'Sec-Fetch-Mode' : 'navigate', 
            'Sec-Fetch-Site' : 'same-origin', 
            'Sec-Fetch-User' : '?1', 
            'Upgrade-Insecure-Requests' : '1', 
            'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0'
            })
        page = sess.get('https://forums.somethingawful.com/forumdisplay.php', params = payload).text
        return page

    def exit_archives(self, fid=1, year=2001, sess=session):
        payload = {
            'forumid' : fid,
            'ac_month' : None,
            'bday_day': None,
            'ac_year': str(year),
            'rem': 'Remove'
            }
        cookie_dict = sess.cookies.get_dict()
        sess_cookie = cookie_dict['__cf_bm']
        sess.headers.update({
            'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8', 
            'Accept-Encoding' : 'gzip, deflate, br', 
            'Accept-Language' : 'en-US,en;q=0.5', 
            'Connection' : 'keep-alive',
            'Cache-Control': 'max-age=0', 
            'Cookie' : f'bbuserid={sa_uid}; bbpassword={sa_bbpwd}; aduserid={sa_aduserid}; __cf_bm={sess_cookie}',
            'Host' : 'forums.somethingawful.com',
            'Sec-Fetch-Dest' : 'document', 
            'Sec-Fetch-Mode' : 'navigate', 
            'Sec-Fetch-Site' : 'same-origin', 
            'Sec-Fetch-User' : '?1', 
            'Upgrade-Insecure-Requests' : '1', 
            'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0'
            })
        page = sess.get('https://forums.somethingawful.com/forumdisplay.php', params = payload).text
        return page

    # LEPER VIEW
    def get_banlist_page(self, p=1, sess=session):
        payload = {
            'sort' : None,
            'asc' : '0',
            'pagenumber' : p
            }
        page = sess.get('https://forums.somethingawful.com/banlist.php', params = payload).text
        return page

    # THREAD PAGE VIEW
    def get_thread_page(self, tid, p=1, sess=session):
        payload = {
            'threadid': tid,
            'pagenumber': p
            }
        page = sess.get('https://forums.somethingawful.com/showthread.php', params = payload).text
        return page

    # USER PROFILE VIEW
    def get_profile(self, uid, sess=session):
        payload = {
            'action': 'getinfo',
            'userid': uid
            }
        page = sess.get('https://forums.somethingawful.com/member.php', params = payload).text
        return page

    # SUBFORUM PAGE VIEW
    def get_subforum_page(self, fid, p=1, sess=session):
        payload = {
            'forumid' : fid,
            'perpage' : '30',
            'sortorder': 'desc',
            'posticon': '0',
            'sortfield': 'lastpost',
            'pagenumber': p
            }
        page = sess.get('https://forums.somethingawful.com/forumdisplay.php', params = payload).text
        return page

    # FORUM INDEX
    def get_forumdisplay(self, fid, sess=session):
        payload = {
            "forumid" : fid
        }
        page = sess.get('https://forums.somethingawful.com/forumdisplay.php', params = payload).text
        return page

    def get_index(sess=session):
        page = sess.get('https://forums.somethingawful.com').text
        return page

    # PRINT TEST PAGE
    def filename(p):
        fn = re.search("<body id=\"something_awful\" class=\"(\w+)\W", p)
        return fn.group(1)

    def print_page(p, f=filename):
        with open(f"docs/{f(p)}.html", 'w') as f:
            f.write(p)
        print("Done!")

class UserCheck:
    def check_if_user_real(self, userid) -> bool:
        payload = {
            'action': 'getinfo',
            'userid': userid
            }
        page = session.get('https://forums.somethingawful.com/member.php', params = payload).text
        return False if re.search("Special Message From El Jefe", str(page)) else True

    def get_uid_from_username(self, username) -> int:
        payload = {
            'action': 'getinfo',
            'username': username
            }
        page = session.get('https://forums.somethingawful.com/member.php', params = payload).text
        thread_error = re.search("Special Message From El Jefe", str(page))
        gateway_error = re.search("Bad Gateway", str(page))

        if thread_error or gateway_error:
            uid = 404
        else:
            uid = (re.search("userid=(\d+)", str(page))).group(1)
        return uid