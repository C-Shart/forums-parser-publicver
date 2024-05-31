from pangres import upsert
import config
import pandas as pd
from typing import Optional, Set
from datetime import datetime
from sqlalchemy import BigInteger, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class SqlTools:
    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(SqlTools, cls).__new__(cls)
        return cls.instance

    db_host = config.host
    db_port = config.port
    db_name = config.database
    db_user = config.user
    db_pass = config.password

    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}")
    connection = engine.connect()

    def get_table_cols(self, table:str, cols:[]=None, condition:str=None):
        if not condition:
            result = pd.read_sql_table(table, self.connection, columns=cols)
        else:
            result = pd.read_sql_query(f"SELECT {','.join(cols)} FROM {table} WHERE {condition}", self.connection)
        return result

    def upsert_df(self, df:pd.DataFrame(), table_name:str, con=connection):
        upsert(con=con, df=df, table_name=table_name, if_row_exists="update")
        con.commit()
        pass


class Base(DeclarativeBase):
    pass

class Forums(Base):
    __tablename__ = "forums"
    forum_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    forum_name: Mapped[str]

    subs: Mapped[Set["Subforums"]] = relationship(back_populates="sub_id")

class Subforums(Base):
    __tablename__ = "subforums"
    forum_id: Mapped[int] = mapped_column()
    sub_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)

    parent: Mapped["Forums"] = relationship(back_populates="forum_id")

class Users(Base):
    __tablename__ = "users"
    userid : Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    username : Mapped[str]
    avatar_url : Mapped[Optional[str]]
    regdate : Mapped[datetime]
    account_type : Mapped[str]
    postcount : Mapped[int]
    post_rate : Mapped[float]
    last_post : Mapped[datetime]
    has_userpic : Mapped[bool]

    threads : Mapped[Set["Threads"]] = relationship(back_populates="userid")
    posts : Mapped[Set["PostMeta"]] = relationship(back_populates="userid")
    gangtags : Mapped[Set["UsersGangtags"]] = relationship(back_populates="userid")
    punishments : Mapped[Set["Leper"]] = relationship(back_populates="userid")
    quotes_in_posts : Mapped[Set["PostQuotes"]] = relationship(back_populates="userid")
    quotes_in_leper : Mapped[Set["LeperQuotes"]] = relationship(back_populates="userid")
    thread_links_from_thread : Mapped[Set["PostLinksThread"]] = relationship(back_populates="userid")
    profile_links_from_thread : Mapped[Set["PostLinksProfile"]] = relationship(back_populates="userid")
    thread_links_from_leper : Mapped[Set["LeperLinksThread"]] = relationship(back_populates="userid")
    profile_links_from_leper : Mapped[Set["LeperLinksProfile"]] = relationship(back_populates="userid")

class UsersGangtags(Base):
    __tablename__ = "users_gangtags"
    p_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    userid : Mapped[int]
    gangtag : Mapped[Optional[str]]

class Threads(Base):
    __tablename__ = "threads"
    thread_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    subforum_id: Mapped[int] = mapped_column()
    thread_title: Mapped[str]
    thread_icon: Mapped[Optional[str]]
    thread_author: Mapped[int] = mapped_column(BigInteger)
    cnt_replies: Mapped[int] = mapped_column(BigInteger)
    cnt_views: Mapped[int] = mapped_column()
    rating: Mapped[Optional[float]]
    votes: Mapped[Optional[int]]
    killed_by: Mapped[str]
    killed_by_date: Mapped[datetime]
    is_closed: Mapped[bool]

    posts : Mapped[Set["PostMeta"]] = relationship(back_populates="post_id")
    # links_from_posts : Mapped[Set["LinksThread"]] = relationship(back_populates="")
    # links_from_leper : Mapped[Set["PostMeta"]] = relationship()

class PostMeta(Base):
    __tablename__ = "post_meta"
    post_id : Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    thread_id : Mapped[int] = mapped_column()
    userid : Mapped[int] = mapped_column() # , primary_key=True
    post_datetime : Mapped[datetime]
    page : Mapped[int]
    per_40_index : Mapped[int]
    cnt_words : Mapped[int]
    edited : Mapped[bool]
    edited_datetime : Mapped[Optional[datetime]]
    edited_by_other : Mapped[Optional[bool]]

    punish : Mapped[Set["Leper"]] = relationship(back_populates="post_id")
    emotes : Mapped[Set["PostEmotes"]] = relationship(back_populates="post_id")
    quotes : Mapped[Set["PostQuotes"]] = relationship(back_populates="post_id")
    images : Mapped[Set["PostEmbedImages"]] = relationship(back_populates="post_id")
    videos : Mapped[Set["PostEmbedVideos"]] = relationship(back_populates="post_id")
    twitts : Mapped[Set["PostEmbedTwitters"]] = relationship(back_populates="post_id")
    youtbs : Mapped[Set["PostEmbedYoutubes"]] = relationship(back_populates="post_id")
    frntpg : Mapped[Set["LinksFrontpage"]] = relationship(back_populates="post_id")
    saclop : Mapped[Set["PostLinksSaclopedia"]] = relationship(back_populates="post_id")
    logout : Mapped[Set["PostLinksLogout"]] = relationship(back_populates="post_id")
    thread : Mapped[Set["PostLinksThread"]] = relationship(back_populates="post_id")
    lleper : Mapped[Set["PostLinksLeper"]] = relationship(back_populates="post_id")
    profil : Mapped[Set["PostLinksProfile"]] = relationship(back_populates="post_id")
    lquote : Mapped[Set["LeperQuotes"]] = relationship(back_populates="post_id")
    lthred : Mapped[Set["LeperLinksThread"]] = relationship(back_populates="post_id")

class PostEmotes(Base):
    __tablename__ = "post_emotes"
    post_id : Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    emote : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    count : Mapped[int]

class PostQuotes(Base):
    __tablename__ = "post_quotes"
    p_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    post_id : Mapped[int] = mapped_column()
    quoted_user : Mapped[Optional[str]] = mapped_column()
    quoted_post : Mapped[Optional[int]] = mapped_column()
    is_external_quote : Mapped[Optional[bool]]
    is_self_quote : Mapped[Optional[bool]]
    is_emptyquote : Mapped[Optional[bool]]

class PostEmbedImages(Base):
    __tablename__ = "post_embed_images"
    p_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    post_id : Mapped[int] = mapped_column()
    image_domain : Mapped[Optional[str]]
    image_filename : Mapped[Optional[str]] = mapped_column()
    image_fileformat : Mapped[Optional[str]]

class PostEmbedVideos(Base):
    __tablename__ = "post_embed_videos"
    p_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    post_id : Mapped[int] = mapped_column()
    video_domain : Mapped[Optional[str]]
    video_filename : Mapped[str] = mapped_column()

class PostEmbedTwitters(Base):
    __tablename__ = "post_embed_twitters"
    p_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    post_id : Mapped[int] = mapped_column()
    twitter_account : Mapped[Optional[str]]
    twitter_post : Mapped[Optional[str]] = mapped_column()

class PostEmbedYoutubes(Base):
    __tablename__ = "post_embed_youtubes"
    p_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    post_id : Mapped[int] = mapped_column()
    video : Mapped[Optional[str]] = mapped_column()

class LinksFrontpage(Base):
    __tablename__ = "post_links_frontpage"
    p_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    post_id : Mapped[int] = mapped_column()
    feature : Mapped[Optional[str]]
    title : Mapped[Optional[str]] = mapped_column()
    page : Mapped[Optional[int]]

class PostLinksSaclopedia(Base):
    __tablename__ = "post_links_saclopedia"
    p_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    post_id : Mapped[int] = mapped_column()
    act : Mapped[Optional[str]]
    l : Mapped[Optional[str]]
    topicid : Mapped[Optional[str]] = mapped_column()

class PostLinksLogout(Base):
    __tablename__ = "post_links_logout"
    p_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    post_id : Mapped[int] = mapped_column()
    link_text : Mapped[Optional[str]] = mapped_column()

class PostLinksThread(Base):
    __tablename__ = "post_links_thread"
    p_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    post_id : Mapped[int] = mapped_column()
    threadid : Mapped[Optional[int]] = mapped_column(BigInteger)
    pageno : Mapped[Optional[int]]
    userid : Mapped[Optional[int]] = mapped_column()
    postid : Mapped[Optional[int]]

class PostLinksLeper(Base):
    __tablename__ = "post_links_leper"
    p_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    post_id : Mapped[int] = mapped_column()
    punished_user : Mapped[Optional[str]] = mapped_column()
    punished_post : Mapped[Optional[int]] = mapped_column()
    approver : Mapped[Optional[int]] = mapped_column()

class PostLinksProfile(Base):
    __tablename__ = "post_links_profile"
    p_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    post_id : Mapped[int] = mapped_column()
    userid : Mapped[Optional[int]] = mapped_column()
    user_exists : Mapped[Optional[bool]]

class Leper(Base):
    __tablename__ = "leper"
    post_id : Mapped[int] = mapped_column()
    punish_type: Mapped[str]
    punish_datetime: Mapped[datetime]
    userid: Mapped[int] = mapped_column()
    punish_length: Mapped[str]
    requestor_id: Mapped[int]
    approver_id: Mapped[int]
    l_index: Mapped[str] = mapped_column(primary_key=True, autoincrement=False)

    emotes : Mapped[Set["LeperEmotes"]] = relationship(back_populates="l_index")
    quotes : Mapped[Set["LeperQuotes"]] = relationship(back_populates="l_index")
    images : Mapped[Set["LeperEmbedImages"]] = relationship(back_populates="l_index")
    videos : Mapped[Set["LeperEmbedVideos"]] = relationship(back_populates="l_index")
    twitts : Mapped[Set["LeperEmbedTwitters"]] = relationship(back_populates="l_index")
    youtbs : Mapped[Set["LeperEmbedYoutubes"]] = relationship(back_populates="l_index")
    thread : Mapped[Set["LeperLinksThread"]] = relationship(back_populates="l_index")
    profil : Mapped[Set["LeperLinksProfile"]] = relationship(back_populates="l_index")

class LeperEmotes(Base):
    __tablename__ = "leper_emotes"
    l_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    emote : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    count : Mapped[int]

class LeperQuotes(Base):
    __tablename__ = "leper_quotes"
    l_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    quoted_user : Mapped[Optional[str]] = mapped_column()
    quoted_post : Mapped[Optional[int]] = mapped_column(primary_key=True, autoincrement=False)

class LeperEmbedImages(Base):
    __tablename__ = "leper_embed_images"
    l_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    image_domain : Mapped[Optional[str]]
    image_filename : Mapped[Optional[str]] = mapped_column(primary_key=True, autoincrement=False)
    image_fileformat : Mapped[Optional[str]]

class LeperEmbedVideos(Base):
    __tablename__ = "leper_embed_videos"
    l_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    video_domain : Mapped[Optional[str]]
    video_filename : Mapped[Optional[str]] = mapped_column(primary_key=True, autoincrement=False)

class LeperEmbedTwitters(Base):
    __tablename__ = "leper_embed_twitters"
    l_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    twitter_account : Mapped[Optional[str]]
    twitter_post : Mapped[Optional[str]] = mapped_column(primary_key=True, autoincrement=False)

class LeperEmbedYoutubes(Base):
    __tablename__ = "leper_embed_youtubes"
    l_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    video : Mapped[Optional[str]] = mapped_column(primary_key=True, autoincrement=False)

class LeperLinksProfile(Base):
    __tablename__ = "leper_links_profile"
    l_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    userid : Mapped[Optional[int]] = mapped_column(primary_key=True, autoincrement=False)
    user_exists : Mapped[Optional[bool]]

class LeperLinksThread(Base):
    __tablename__ = "leper_links_thread"
    l_index : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    threadid : Mapped[str] = mapped_column(primary_key=True, autoincrement=False)
    pageno : Mapped[Optional[int]]
    userid : Mapped[Optional[int]] = mapped_column()
    postid : Mapped[Optional[int]] = mapped_column()

""" this = ()
sql = SqlTools()
forum_ids = sql.get_table_cols("forums", ["forum_id"])
print(forum_ids) """

s = SqlTools()
Base.metadata.create_all(s.engine)