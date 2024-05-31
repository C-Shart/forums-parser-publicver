from spiderman import Crawlers

crawl = Crawlers()
# db = SqlTools()

### RETRIES
# crawl.crawl_thread(3163064, page_no=1)
# crawl.crawl_subforum(686)
# crawl.crawl_profile(104386)
# crawl.crawl_archive_page(14, 22, 2004)


### ONE TIME (?)
# crawl.generate_indexes()
# crawl.populate_archives()

### BACKFILLS
# crawl.backfill_archives()
# crawl.backfill_backfill_threads()
# crawl.final_backfill_threads()

### EXECUTION
crawl.populate_subforums()
crawl.populate_post_tables()


# Saving this particular one for posterity. This is the archived
# thread that forced me to change some columns to BIGINT
# 
# crawl.crawl_archive_page(154, 153, 2007)
# 



### BACKFILLS COMPLETE
# crawl.update_threads_and_posts()
# crawl.backfill_closed_status(21)
# crawl.backfill_users_gangtags()
# crawl.backfill_post_links_threads()
