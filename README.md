# SSREQUESTER
Request and parse data from popular in Latvia ss.com site.

#### Configuration
- sites - Array of comma separated strings to be requested, direct links to pages. 
- report - Boolean, generate result report.
- upload - Boolean, upload results to DB.
- export - Boolean, export to file. Look into "export.filename".
- export.filename - String, file name to be generated. Ex. ads.json.


- sscom.url - String, urls that needs to be cutted out inside pages.
- sscom.class.url - String, class name that represents URLs on a page.
- sscom.class - String, class name that represents items on a page.
- sscom.parser.config - Object, custom configuration of HTMLParser.


- logging.name - String, logger name. 
- logging.format- String, logging format.
- logging.file - String, logging file name.
- logging.level - Integer, logging level.


- db.url - String, mongo database connection string in case of upload: true.

- address.field - String, name of address field.

- restart - Integer, timeout to restart, seconds.

#### StartUp
- Install pm2.
- Execute run.sh