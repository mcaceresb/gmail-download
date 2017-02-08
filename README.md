Description
-----------

Query gmail e-mail for a specified date range (today only is the
default). Output is saved to a subfolder named after the requested start
date within a specified target output folder. You can also specify
"sorting" rules to pre-classify your e-mail into folders.

Setup
-----

Dependencies
* BeautifulSoup (bs4)
* apiclient
* oauth2client
* pandas
* httplib2
* bitmath

You also need to set up Gmail to query it from python. See the
[Gmail API quickstart](https://developers.google.com/gmail/api/quickstart/python)
for instructions. Once you have the Gmail API set up, run
```bash
mkdir $HOME/lib
cd $HOME/lib
git clone github.com/mcaceresb/gmail-download
cd gmail-download
chmod +x ./gmail_query.py
./gmail_query.py --setup
```

The last line will guide you through creating `$HOME/.gmail_query.conf`; the file will look like this:
```
[Gmail]
    email   = my.gmail.address@gmail.com
    secret  = /path/to/client_secret.json
    appname = Gmail API name

[Setup]
    output_folder          = /gmail/default/download/folder
    output_type            = html
    download_attachments   = True
    max_attachment_size    = 20MiB
    query_days             = 1
    threaded_first         = True
    notify_email           = False
    sorting_rules          = /path/to/sorting/rules.json
    sorting_case_sensitive = False
```

The options under `[Gmail]` are required. The options under `[Setup]` are optional and can be
changed in the program call (they merely represent the defaults).

Usage
-----

```bash
gmail-query.py
gmail-query.py -d 2016-06-01 -o ~/Downloads/email
gmail-query.py --mail --days-back 7 --first \
    --sort-rules gmail_rules.json --output-type html
```

Though intended to be used from the command line, one can run the query from ython
```python
from gmail_query import gmail_query
from dateutil import tz
query = gmail_query(outdir = '/path/to/output')
query.query()
query.query(todays = '2016-06-01')
query.query(bdays      = 7,
            mail       = True,
            first      = True,
            out_type   = 'html',
            sort_rules = 'gmail_rules.json'):
```

Documentation
-------------

### Sorting rules

Sorting rules are meant to classify e-mail based on regexes. The file is
a JSON file formatted as follows

```javascript
{
    "folder1": {
        "rules": ["regexp?",
                  "cookies"],
        "priority": 0
    },
    "folder2": {
        "rules": ["^(from|cc|bcc|to):.*cookies@(gmail\\.com|yahoo\\.com)",
                  "^subject:.*cookies"],
        "priority": 99
    }
}
```

This specifies whether to move e-mails matching any of the specified
rules into folders of the rule set's name. The moves happen in the order
specified by `priotity`. In the example above, everything matching the
regexes `regexp?` OR `cookies` into `folder1`, a subfolder which will
be created in the output folder. Then it would move everything from or
to `cookies@gmail.com` and `cookies@yahoo.com` to `folder2`.

Note downloaded e-mails will start with from, to, cc, bcc, subject,
date, and content statements, meaning that one can sort based on those
by prepending `^(from|cc|bcc|to|subject|content-type):` to a given rule.

Searches are *case insensitive* by default.

### Config file

There are two sets of options. First, Gmail options which are determined
when you set up the [Gmail API](https://developers.google.com/gmail/api/quickstart/python)
- email, your gmail account email.
- secret, your secret API file.
- appname, the app name you choose for this instance of the Gmail API.

Then there are options that the program will assume as default when
run. All these options can be changed when running the program, but
unless specified the option in the `.conf` file will be used

- `output_folder`: A file path to the default ouptut folder to download e-mail to.
- `output_type`: One of 'plain', 'html', 'markdown', or 'eml', the default file type.
- `download_attachments`: 'True' or 'False', whether to download attachments.
- `max_attachment_size`: largest attachment size to download. This tolerates any string format that can be parsed
by `bitmath.parse_string` (e.g. 5MiB, 2KiB, 1.7GiB, etc.)
- `query_days`: Integer, the number of days backwards from the date specified to query e-mail (e.g. 7 queries the last week).
- `threaded_first`: 'True' or 'False', whether to thread e-mails using the first-email downloaded for a given thread (otherwise it uses the latest downloaded e-mail for the thread).
- `notify_email`: 'True' or 'False', whether to notify you via e-mail that this script ran.
- `sorting_rules`: A file path to the sorting rules to use to classify e-mail once downloaded.
- `sorting_case_sensitive`: 'True' or 'False', Whether the regexes in `sorting_rules` should be case sensitive.

### Main function

```bash
usage: gmail_query.py [-h] [--auth_host_name AUTH_HOST_NAME]
                      [--noauth_local_webserver]
                      [--auth_host_port [AUTH_HOST_PORT [AUTH_HOST_PORT ...]]]
                      [--logging_level {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
                      [-o OUT] [-d DATE] [-b days_back] [-e ext] [-f] [-m]
                      [--html] [-s] [--sort-rules SORT_RULES]

optional arguments:
  -h, --help            show this help message and exit
  --auth_host_name AUTH_HOST_NAME
                        Hostname when running a local web server.
  --noauth_local_webserver
                        Do not run a local web server.
  --auth_host_port [AUTH_HOST_PORT [AUTH_HOST_PORT ...]]
                        Port web server should listen on.
  --logging_level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Set the logging level of detail.
  -o OUT, --output OUT  Output folder.
  -d DATE, --date DATE  Date to query.
  -b days_back, --days-back days_back
                        Days back to query e-mail.
  -e ext, --ext ext     File extension.
  -f, --first           Save by first message in thread.
  -m, --mail            Send notification e-mail.
  --sort-rules SORT_RULES
                        File with sorting rules.
```

Notes
-----

While I have made an effort to make this script platform independent, I
have only tested it on my local Linux machine.

I realize that pandas is a rather annoying dependence to have, but I
wanted to download threaded messages into the same subfolder and using
pandas seemed like the easiest way to group messages by thread. Feel
free to modify the code if you have a better idea.

Further, since I download messages by thread, I don't know a priori the
depth of the message thread or whether a "thread" is really a single
message. Hence all the awkward try/except pairs that try to find the
messages within a thread: An e-mail object can be a message or a thread
and this needs to handle both.

TODO
----

* Change behavior to query all threads occurring in time span and then download all e-mails associated with thread? Maybe add option for this.
* Actually allow one to install this using a config file
* Update code so one can output to plain text, html, markdown, eml
* Update code so one can specify whether to download attachments and max attachment size
* Allow case-sensitive sorting rules.
* Update options so sorting rules:
    - Are ignored if no file is provided
    - Are given a warning if the file does not exist
    - Are given a warning if the file is in the wrong format?
* Document all the things! Including config file and CLI options
* Add various options from the Gmail API (currently this downloads ALL your e-mails)
