## PRI 0
* 11/03/23 - 3 of 5 accounts banned | TOTAL PROGRESS: 12.2M posts, 2,522,518 threads
    * reimplement sleep randomizer
    * Try unbanning single account to test the waters with slower requests.
* Get foobardog credentials
* error limits
* catch errors/exit
* catch banned/exit

## CURRENT STATE:
* Threads completely crawled. Only updating ~ 1x/wk going forward.
* Retry module expanded. Should probably rename it soon
* member.php is in the do not crawl list, lol oops oh well

## TODO:
* Error handling - IP
* Testing/unit tests - AWACT

## BACKFILLS
* ~~post_links_thread for rows where userid, threadid, AND pageno columns are all blank~~ - DONE
    * However, I fucked up deleting duplicates, and have to re-do the first however many threads in the table, but that's for later.
* post_meta.edited_by_other status - re-check for all in edited_backfills.csv - AWACT
* post_links_leper.approver -- KIND OF A PITA, MIGHT JUST LET THIS SHITTY DATA RIDE
    * For all rows where approver == empty string, get thread_ids
    * Truncate post_links_leper.approver
    * Alter column approver to be int type
    * Re-check for all in (post_links_leper where approver=="")

## NEXT STEPS
* Build crawling function(s) & logic
    * Forum indexes: DONE
    * Thread indexes: DONE
    * User profiles: DONE
    * Posts: DONE+
    * Leper: AWACT

## LAST STEPS
* Anonymize the complete db
* No need for anonymizing updater, will simply end the gathering at a point and it will remain static.

## Stretch Goals
* SAclopedia parser(/ARCHIVER?)
    * Write the pages to an archive
    * Title, posted by, date

## REDOS
* First 3.16 million rows - edited fields logic was wrong
* First 2 mil -- other backfills?