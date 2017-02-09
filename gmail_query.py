#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""Query e-mail from gmail

Query gmail e-mail for a specified date range (today only is the
default). Output is saved to a subfolder named after the requested start
date within a specified target output folder. You can also specify
"sorting" rules to pre-classify your e-mail into folders.

To get started (and set up Gmail to allow you to do this) see

https://developers.google.com/gmail/api/quickstart/python

Usage
-----

# From CLI
$ gmail-query.py
$ gmail-query.py -d 2016-06-01
$ gmail-query.py -d 2016-06-01 -o ~/Downloads/email

# From Python
>>> from gmail_query import gmail_query
>>> from dateutil import tz
>>> query = gmail_query(outdir = '/path/to/output')
>>> query.query()
>>> query.query(todays = '2016-06-01', bdays = 7)

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
"""

from __future__ import division, print_function
from dateutil.parser import parse
from bitmath import parse_string
from operator import itemgetter
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from dateutil import tz
from shutil import move
from os import path
import pypandoc as pandoc
import pandas as pd
import oauth2client
import httplib2
import datetime
import base64
import string
import json
import sys
import os
import re

# Python 2/3 compat
# -----------------

try:
    from ConfigParser import ConfigParser, RawConfigParser
except:
    from configparser import ConfigParser, RawConfigParser

try:
    input = __builtins__.raw_input
except:
    raw_input = input

try:
    unicode is unicode
except NameError:
    def unicode(x):
        return str(x, 'utf-8')

# ---------------------------------------------------------------------
# Main function wrapper

cfgfile  = path.join(path.expanduser('~'), '.gmail_query.conf')
ext_dict = {'eml': '.eml',
            'docx': '.docx',
            'html': '.html',
            'html5': '.html',
            'json': '.json',
            'latex': '.tex',
            'markdown': '.md',
            'markdown_github': '.md',
            'markdown_mmd': '.md',
            'markdown_phpextra': '.md',
            'markdown_strict': '.md',
            'plain': '.txt',
            'rst': '.rst'}

def main():
    cfg_init(cfgfile)
    def_args = args_fallback()
    cfg_args = args_config(cfgfile, def_args)
    cli_args = args_cli(cfg_args)

    query = gmail_query(cli_args.outdir, flags = cli_args)
    query.query(todays  = cli_args.date,
                bdays   = cli_args.bdays,
                otype   = cli_args.otype,
                ext     = cli_args.ext,
                att_get = cli_args.att_get,
                att_max = cli_args.att_max,
                mail    = cli_args.mail,
                first   = cli_args.first,
                sort_case  = cli_args.sort_case,
                sort_rules = cli_args.sort_file)

# ---------------------------------------------------------------------
# Create .conf file, update .conf file

def cfg_init(cfgfile):
    try:
        setup = sys.argv[1] == 'setup'
    except:
        setup = False

    if not path.isfile(cfgfile) or setup:
        cfgopts = {'Gmail.email': ["regex", "[^@]+@[^@]+\.[^@]+"],
                   'Gmail.secret': ["file", ""],
                   'Gmail.appname': ["anything", ""],
                   'Setup.output_folder': ["anything", ""],
                   'Setup.output_type': ["regex", '|'.join(ext_dict.keys())],
                   'Setup.output_ext': ["anything", ""],
                   'Setup.download_attachments': ["regex", "True|False"],
                   'Setup.max_attachment_size': ["anything", ""],
                   'Setup.query_days': ["regex", "\d+"],
                   'Setup.threaded_first': ["regex", "True|False"],
                   'Setup.notify_email': ["regex", "True|False"],
                   'Setup.sorting_rules': ["file", ""],
                   'Setup.sorting_case_sensitive': ["regex", "True|False"]}

        if not path.isfile(cfgfile):
            cfgparser    = RawConfigParser()
            msg_notfound = "~/.gmail_query.conf not found. Create one? [Y/n] "
            cfg_action   = 'new'
        elif len(sys.argv) > 2:
            cfg_action   = 'update'
            cfg_setting  = sys.argv[2]
            try:
                cfg_how, cfg_check = cfgopts[cfg_setting]
            except:
                raise Warning("Setting '{}' not available".format(cfg_setting))

            try:
                cfg_value = sys.argv[3]
            except:
                raise Warning("Provider a value for '{}'".format(cfg_setting))

            if cfg_how == 'regex':
                if not re.match(cfg_check, cfg_value):
                    cfg_match = (cfg_setting, cfg_check)
                    raise Warning("'{}' should match '{}'".format(*cfg_match))
            elif cfg_how == 'file':
                if not path.isfile(cfg_setting):
                    raise Warning("'{}' does not exist.".format(cfg_value))

            cfgparser = ConfigParser()
            cfgparser.read(cfgfile)
            cfg_section, cfg_option = cfg_setting.split('.')

            if cfg_section not in cfgparser.sections():
                cfgparser.add_section(cfg_section)

            cfgparser.set(cfg_section, cfg_option, cfg_value)
            with open(cfgfile, 'w') as cfg:
                cfgparser.write(cfg)

            sys.exit()

        elif len(sys.argv) > 4:
            raise Warning("Can only update one option at a time.")
        else:
            cfg_action   = 'replace'
            msg_notfound = "Replace ~/.gmail_query.conf? [y/N] "

        opt_notfound = ['y', 'yes', 'n', 'no', '']
        msg_email    = "What is your e-mail address? "
        opt_email    = "[^@]+@[^@]+\.[^@]+"
        msg_secret   = "Where is your client_secret.json file? "
        msg_appname  = "What is the name of your Gmail API app? "

        cont_yn = input(msg_notfound)
        while cont_yn.lower() not in opt_notfound:
            cont_yn = input("Please answer 'y' or 'n'. " + msg_notfound)

        action = ['n', 'no'] + [] if cfg_action == 'new' else ['']
        if cont_yn.lower() in action:
            print('You need to create a configuration file. See help.')
            sys.exit()

        my_email = input(msg_email)
        while not re.findall(opt_email, my_email):
            msg_err  = "Enter a valid email. " + msg_email
            my_email = input(msg_err)

        my_secret = input(msg_secret)
        while not path.isfile(my_secret):
            msg_err   = "'{}' not found. ".format(my_secret) + msg_secret
            my_secret = input(msg_err)

        my_appname = input(msg_appname)
        while my_appname == '':
            msg_err   = "Provider a name. ".format(my_appname) + msg_appname
            my_secret = input(msg_err)

        print(os.linesep + "You can also specify the default output folder.")
        def_outdir = input("(press <enter> to skip): ")

        with open(cfgfile, 'w') as cfg:
            cfg.write("[Gmail]" + os.linesep)
            cfg.write("email   = {}".format(my_email + os.linesep))
            cfg.write("secret  = {}".format(my_secret + os.linesep))
            cfg.write("appname = {}".format(my_appname + os.linesep))
            cfg.write(os.linesep)

            if def_outdir != '':
                cfg.write("[Setup]" + os.linesep)
                cfg.write("output_folder = {}".format(def_outdir + os.linesep))

        sys.exit(0)

# ---------------------------------------------------------------------
# Parse fallback options

class args_fallback():

    """Fallback default arguments"""

    def __init__(self):
        self.outdir    = ''
        self.bdays     = 0
        self.otype     = 'html'
        self.ext       = ''
        self.att_get   = False
        self.att_max   = '20MiB'
        self.mail      = False
        self.first     = False
        self.sort_file = ''
        self.sort_case = False
        self.sort      = False

# ---------------------------------------------------------------------
# Parse config file options

class args_config():

    """Parse arguments from configuration file"""

    def __init__(self, cfgfile, fallback):
        cfgparser = ConfigParser()
        cfgparser.read(cfgfile)

        # Required
        # --------

        msg = 'Add {} = {} under [{}] in ~/.gmail_query.conf'
        try:
            self.my_email = cfgparser.get('Gmail', 'email')
        except:
            raise Warning(msg.format('email', 'g-mail', 'Gmail'))

        try:
            self.my_secret = cfgparser.get('Gmail', 'secret')
        except:
            raise Warning(msg.format('secret', 'client_secret.json', 'Gmail'))

        try:
            self.my_appname = cfgparser.get('Gmail', 'appname')
        except:
            raise Warning(msg.format('appname', 'API name', 'Gmail'))

        # Optional
        # --------

        try:
            self.outdir = cfgparser.get('Setup', 'output_folder')
        except:
            self.outdir = fallback.outdir

        try:
            self.otype = cfgparser.get('Setup', 'output_type')
        except:
            self.otype = fallback.otype

        try:
            self.ext = cfgparser.get('Setup', 'output_ext')
        except:
            self.ext = fallback.ext

        try:
            self.att_get = cfgparser.getboolean('Setup',
                                                'download_attachments')
        except:
            self.att_get = fallback.att_get

        try:
            self.att_max = cfgparser.get('Setup', 'max_attachment_size')
        except:
            self.att_max = fallback.att_max

        try:
            self.bdays = cfgparser.getint('Setup', 'query_days')
        except:
            self.bdays = fallback.bdays

        try:
            self.first = cfgparser.getboolean('Setup', 'threaded_first')
        except:
            self.first = fallback.first

        try:
            self.mail = cfgparser.getboolean('Setup', 'notify_email')
        except:
            self.mail = fallback.mail

        try:
            self.sort_file = cfgparser.get('Setup', 'sorting_rules')
        except:
            self.sort_file = fallback.sort_file

        try:
            self.sort_case = cfgparser.getboolean('Setup',
                                                  'sorting_case_sensitive')
        except:
            self.sort_case = fallback.sort_case

# ---------------------------------------------------------------------
# Parse CLI arguments


class args_cli():

    """Parse command-line arguments"""

    def __init__(self, defaults):

        import argparse
        parser = argparse.ArgumentParser(parents = [tools.argparser])

        if defaults.outdir == '':
            parser.add_argument('-o', '--output',
                                dest     = 'out',
                                type     = str,
                                nargs    = 1,
                                metavar  = 'OUT',
                                help     = "Output folder.",
                                required = True)
        else:
            parser.add_argument('-o', '--output',
                                dest     = 'out',
                                type     = str,
                                nargs    = 1,
                                default  = [defaults.outdir],
                                metavar  = 'OUT',
                                help     = "Output folder.",
                                required = False)

        parser.add_argument('-d', '--date',
                            dest     = 'date',
                            type     = str,
                            nargs    = 1,
                            metavar  = 'DATE',
                            default  = [None],
                            help     = "Date to query.",
                            required = False)

        parser.add_argument('-t', '--output-type',
                            dest     = 'otype',
                            type     = str,
                            nargs    = 1,
                            metavar  = 'OUTPUT_TYPE',
                            default  = [defaults.otype],
                            help     = "Output type.",
                            required = False)

        parser.add_argument('-e', '--ext',
                            dest     = 'ext',
                            type     = str,
                            nargs    = 1,
                            metavar  = 'ext',
                            default  = [defaults.ext],
                            help     = "File extension.",
                            required = False)

        parser.add_argument('-a', '--attachments',
                            dest     = 'attachments',
                            action   = 'store_true',
                            help     = "Download attachments.",
                            required = False)

        parser.add_argument('--attachment-max-size',
                            dest     = 'max_size',
                            type     = str,
                            nargs    = 1,
                            metavar  = 'MAX_SIZE',
                            default  = [defaults.att_max],
                            help     = "Largest attachment size.",
                            required = False)

        parser.add_argument('-b', '--days-back',
                            dest     = 'days_back',
                            type     = int,
                            nargs    = 1,
                            metavar  = 'DAYS_BACK',
                            default  = [defaults.bdays],
                            help     = "Days back to query e-mail.",
                            required = False)

        parser.add_argument('-f', '--first',
                            dest     = 'first',
                            action   = 'store_true',
                            help     = "Save by first message in thread.",
                            required = False)

        parser.add_argument('-m', '--mail',
                            dest     = 'mail',
                            action   = 'store_true',
                            help     = "Send notification e-mail.",
                            required = False)

        parser.add_argument('--sort-rules',
                            dest     = 'sort_rules',
                            type     = str,
                            nargs    = 1,
                            metavar  = 'SORT_RULES',
                            default  = [defaults.sort_file],
                            help     = "File with sorting rules.",
                            required = False)

        parser.add_argument('--case-sensitive',
                            dest     = 'case',
                            action   = 'store_true',
                            help     = "Sorting rules are case-sensitive.",
                            required = False)

        self.flags     = parser.parse_args()
        self.outdir    = os.path.expanduser(self.flags.out[0])
        self.date      = self.flags.date[0]
        self.otype     = self.flags.otype[0]
        self.ext       = self.flags.ext[0]
        self.att_get   = self.flags.attachments or defaults.att_get
        self.att_max   = self.flags.max_size[0]
        self.bdays     = self.flags.days_back[0]
        self.first     = self.flags.first or defaults.first
        self.mail      = self.flags.mail or defaults.mail
        self.sort_file = os.path.expanduser(self.flags.sort_rules[0])
        self.sort_case = self.flags.case or defaults.sort_case
        self.sort      = self.sort_file != ''

# ---------------------------------------------------------------------
# Main query wrapper

class gmail_query():

    """Query gmail e-mail for the day

    Query gmail e-mail for a specified date range (today only is
    the default). Output is saved to a subfolder named after the
    requested start date within a specified target output folder.
    You can also specify "sorting" rules to pre-classify your e-mail
    into folders. To get started see

    https://developers.google.com/gmail/api/quickstart/python

    Usage
    -----

    >>> from gmail_query import gmail_query
    >>> from dateutil import tz
    >>> query = gmail_query(outdir = '/path/to/output')
    >>> query.query()
    >>> query.query('2016-01-01')
    """

    def __init__(self, outdir, flags = None, cfgfile = cfgfile):
        """Query gmail e-mail for the day

        Kwargs:
            outdir: Output directory
        """

        def_args = args_fallback()
        cfg_args = args_config(cfgfile, def_args)

        self.outmail  = cfg_args.my_email
        self.outdir   = outdir
        self.timezone = tz.tzlocal()
        self.tzstr    = datetime.datetime.now(self.timezone).tzname()

        # API info
        # --------

        client_secret_file = cfg_args.my_secret
        app_name = cfg_args.my_appname
        scopes   = ['https://www.googleapis.com/auth/gmail.readonly',
                    'https://www.googleapis.com/auth/gmail.insert']

        # Create gmail messages object
        # ----------------------------

        credentials   = get_credentials(app_name,
                                        client_secret_file,
                                        scopes,
                                        flags)
        http          = credentials.authorize(httplib2.Http())
        service       = discovery.build('gmail', 'v1', http    = http)
        self.messages = service.users().messages()
        self.cfg_args = cfg_args

    def query(self,
              todays  = None,
              bdays   = None,
              otype   = None,
              ext     = None,
              att_get = None,
              att_max = None,
              mail    = None,
              first   = None,
              sort_case  = None,
              sort_rules = None):

        """Query Gmail e-mail for specified date

        Kwargs:
            todays: Date to query (e.g. 2016-01-01; default today)
            bdays: Days back to look for e-mail.
            mail: Give yourself e-mail notification with query results
            ext: Email file extension (default blank)

        Returns:
            Output todays email to specified output folder and prints or
            e-mails message with query results.
        """

        if bdays is None:
            bdays = self.cfg_args.bdays

        if otype is None:
            otype = self.cfg_args.otype

        if ext is None:
            ext = self.cfg_args.ext

        if att_get is None:
            att_get = self.cfg_args.att_get

        if att_max is None:
            att_max = self.cfg_args.att_max

        if mail is None:
            mail = self.cfg_args.mail

        if first is None:
            first = self.cfg_args.first

        if sort_case is None:
            sort_case = self.cfg_args.sort_case

        if sort_rules is None:
            sort_rules = self.cfg_args.sort_file

        ptypes = pandoc.get_pandoc_formats()[1]
        if otype not in ptypes and not otype == 'eml':
            raise Warning("Output type must be: {}".format(', '.join(ptypes)))

        max_size = parse_string(att_max) if att_get else None
        sort = sort_rules != ''

        # Get date to query, recursively create output dir
        # ------------------------------------------------

        todays = todays if todays else str(datetime.date.today())
        outdir = os.path.join(self.outdir, todays)
        self.finaldir = outdir
        mkdir_recursive(outdir)

        # Query Gmail
        # -----------

        try:
            df  = self.query_todays(todays, bdays, first, otype, max_size)
        except:
            df  = None
            res = "Gmail query FAILED"

        ext = ext_dict[otype] if ext == '' else ext
        if df is not None:
            print_df_query(df, outdir, self.tzstr, otype, ext)
            if sort:
                if os.path.isfile(sort_rules):
                    try:
                        self.sort_query(sort_rules, sort_case)
                    except:
                        print("Sorting failed. Check '{}'".format(sort_rules))
                else:
                    print("'{}' not found. Can't sort.".format(sort_rules))
            res = "Success! See output folder:" + os.linesep + outdir
        else:
            res = 'No e-mail %s' % todays

        # Report success/fail
        # -------------------

        if mail:
            msg = ["Content-Type: text/plain; charset=\"UTF-8\"",
                   "MIME-Version: 1.0",
                   "Content-Transfer-Encoding: message/rfc2822",
                   "To: <%s>" % self.outmail,
                   "From: <%s>" % self.outmail,
                   "Subject: Mail Dump for %s" % todays, "", res]

            b = base64.b64encode(os.linesep.join(msg))
            b = b.replace('+', '-').replace('/', '_')
            self.messages.insert(userId = 'me', body = {'raw': b}).execute()
        else:
            print(res)

    def query_todays(self, todays, bdays, first, otype, msize):
        """Get all of today's messages

        Args:
            todays: Messages from date.
            bdays: Days back to look for e-mail.

        Returns:
            df: Data frame with today's messages

        """

        # Gmail queries date >= after and date < before
        todaydt  = datetime.datetime.strptime(todays, "%Y-%m-%d")
        tomorrow = todaydt + datetime.timedelta(days = 1)
        tomorrow = tomorrow.strftime("%Y-%m-%d")
        todaydt  = todaydt + datetime.timedelta(days = -bdays)
        todays   = todaydt.strftime("%Y-%m-%d")
        query    = "after:%s before:%s" % (todays, tomorrow)

        # Query today's message; if no messages, return None
        todaym = self.messages.list(userId = 'me', q = query,
                                    maxResults = 1000).execute()
        if not todaym:
            return None

        # Loop through all messages; get message body and attachments
        msg_ids  = set([ids['id'] for ids in todaym['messages']])
        all_msgs = [self.get_msg(mid) for mid in msg_ids]
        thr_ids  = [msg['threadId'] for msg in all_msgs]
        atts     = [self.parse_att(msg, msize) for msg in all_msgs]
        parsed   = [self.parse_msg(msg, otype) for msg in all_msgs]

        # Return messages as pandas data frame
        cols  = ['threadId',
                 'body',
                 'ft_header',
                 'header',
                 'date',
                 'subject',
                 'fn',
                 'att']
        dtzip = zip(thr_ids, parsed, atts)
        dfstr = '%Y-%m-%d %H:%M:%S ' + self.tzstr
        dt    = [[thr] + pmsg + att for thr, pmsg, att in dtzip]
        df    = pd.DataFrame(dt, index = msg_ids, columns = cols)

        # Parse string dates as datetime
        df['dtf'] = df['date'].apply(lambda d: d.strftime(dfstr))
        df.sort_values(by = ['threadId', 'dtf'],
                       inplace = True,
                       ascending = [True, not first])

        return df

    def sort_query(self, sort_rules, case):
        """Sort queried e-mail into sub-folders using sort_rules

        Args:
            sort_rules: JSON file with rules.

        Returns:
            Each key in sort_rules is a sub-folder within outdir. The
            program recursively searches all e-mail threads in outdir
            for files within each thread matching a rule.

            If a file in the thread matches any of the keys' rules, it
            moves it to outdir/key. If it matches no rules, it is moved
            to outdir/unsorted.

            The search is applied in order using each key's priority.
            Keys with equal priority are applied in arbitrary order.
        """

        outdir  = self.finaldir
        srules  = json.load(open(sort_rules))
        outwalk = os.walk(outdir)

        outwalk_static = []
        for root, dirs, files in outwalk:
            outwalk_static += [[root, dirs, files]]

        unsorted = mkdir_recursive(os.path.join(outdir, "unsorted"))
        for root, dirs, files in outwalk_static:
            if len(files) > 0:
                for fname in files:
                    if apply_rules(srules,
                                   outdir,
                                   root,
                                   fname,
                                   case) in srules.keys():
                        break

                if os.path.isdir(root):
                    move(root, unsorted)

    def parse_att(self, msg, msize, depth = 10):
        """Get all attachments in message thread

        Args:
            msg: gmail msg
            msize: A bitmath object with max size

        Kwargs:
            depth: how deep to look for parts in payload

        Returns:
            All attachments found in msg, or None

        """

        if msize is None:
            return [None, None]

        parts    = msg['payload']
        att_fn   = None
        att_data = None
        found    = False
        try:
            i = 0
            while not found and i < depth:
                i += 1
                parts, found = get_next_part(parts,
                                             search   = 'filename',
                                             negation = True,
                                             allowed  = u'')
        except:
            parts = [{'filename': None}]

        for part in parts:
            if part['filename']:
                att_fn = part['filename']
                att_id, att_size = part['body'].values()
                att_bm = parse_string('{:.9f}B'.format(att_size)).best_prefix()
                if att_size < msize.bytes:
                    att  = self.get_att(msg['threadId'], att_id)
                    body = unicode(att['data']).encode('utf-8')
                    att_data = body
                else:
                    msg_size  = 'NOTE: Att size was %s but limit set to %s'
                    msize_str = msize.format("{value:.1f} {unit}")
                    att_str   = att_bm.format("{value:.1f} {unit}")
                    att_data  = msg_size % (att_str, msize_str)
                    att_fn   += ' [ATTACHMENT T0O LARGE]'

        return [att_fn, att_data]

    def parse_msg(self, msg, otype, prefer = 'text/html', depth = 10):
        """Get body from message, various formats

        Args:
            message: dictionary with message info from Gmail API

        Kwargs:
            prefer: prefer this type
            depth: how deep to look for parts in payload

        Returns: Plain text e-mail exchange
        """

        types = ['text/html', 'text/plain']
        if prefer not in types:
            raise Warning("Can only search for text/plain or text/html.")

        # Find the message body
        found = False
        parts = msg['payload']

        try:
            i = 0
            while not found and i < depth:
                parts, found = get_next_part(parts, allowed = types)
                i += 1

            for p in parts:
                if p['mimeType'] == prefer:
                    break

            pmime = p['mimeType']
            body  = p['body']['data']
            plain = base64.urlsafe_b64decode(unicode(body).encode('utf-8'))
        except:
            pmime = 'text/plain'
            plain = 'Message body could not be retrieved.'

        if found and pmime != prefer:
            msg_type = 'Could not find preferred type. Body retrieved as %s.'
            print(msg_type % pmime)

        # Get headers
        head = dict((h['name'], h['value']) for h in msg['payload']['headers'])
        fr   = get_key_set(head, ['From', 'from', 'FROM'], 'Unknown')
        to   = get_key_set(head, ['To', 'to', 'TO'], 'Unknown')
        cc   = get_key_set(head, ['Cc', 'cc', 'CC'], None)
        sub  = get_key_set(head, ['Subject', 'subject', 'SUBJECT'], 'Unknown')

        # Get message date
        datef = str(datetime.datetime.today())
        try:
            dateu = parse(get_key_set(head, ['Date', 'date', 'DATE'], datef))
        except:
            dateu = datetime.datetime.today()

        try:
            datel = dateu.astimezone(self.timezone)
        except:
            datel = dateu.replace(tzinfo = self.timezone)

        dates = datel.strftime('%a, %d %b %Y %H:%M:%S ' + self.tzstr)

        # Format headers
        ctype = 'html' if otype == 'eml' else otype
        head  = ['**From:** ' + fr,
                 '**To:** ' + to,
                 '**Cc:** ' + cc if cc else cc,
                 '**Subject:** ' + sub,
                 '**Date:** ' + dates,
                 '**Id:** ' + msg['id'],
                 '**Content-type:** ' + pmime]

        md_head    = ('  ' + os.linesep).join(filter(None, head))
        plain_head = os.linesep.join(filter(None, head)).replace('*', '')
        ft_head    = pandoc.convert_text(md_head, ctype, format = 'markdown')
        ft_body    = pandoc.convert_text(plain, ctype,
                                         format     = 'html',
                                         extra_args = ['--smart'])

        return [ft_body, ft_head, plain_head, datel, sub]

    def get_msg(self, msg_id):
        return self.messages.get(userId = 'me',
                                 id     = msg_id,
                                 format = 'full').execute()

    def get_att(self, msg_id, att_id):
        return self.messages.attachments().get(userId = 'me',
                                               messageId = msg_id,
                                               id = att_id).execute()

def get_credentials(app_name, client_secret_file, scopes, flags = None):
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """

    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)

    credential_path = os.path.join(credential_dir,
                                   'gmail-python-quickstart.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(client_secret_file, scopes)
        flow.user_agent = app_name
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else:  # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)

        print('Storing credentials to ' + credential_path)

    return credentials

def get_next_part(msg, what = 'parts', search = 'mimeType',
                  negation = False, allowed = ['text/html', 'text/plain']):
    """Find the next level down of msg

    Args:
        msg: Dictionary object

    Kwargs:
        what: Key to find.
        search: Key criteria.
        allowed: If key not in allowed, search deeper.

    Returns:
        Next level (or current level if fount) as a list and True or
        False if search was in allowed

    """
    if type(msg) is not list:
        if negation:
            criteria = msg[search] not in allowed
        else:
            criteria = msg[search] in allowed

        if criteria:
            return [msg], True
        else:
            parts = msg[what]
    else:
        for m in msg:
            if negation:
                criteria = m[search] not in allowed
            else:
                criteria = m[search] in allowed

            if m[search] in allowed:
                return msg, True

        parts = msg[0][what]

    if type(parts) is not list:
        parts = [parts]

    return parts, False

def get_key_set(dictionary, keys, fallback = None):
    """Get first dictionary element in keys

    Args:
        dictionary: dictionary
        keys: keys to look for

    Returns:
        First key from keys found, pass, or fallback
    """

    for key in keys:
        try:
            return dictionary[key]
        except:
            pass

    if fallback is None:
        pass
    else:
        return fallback

def print_df_query(df, outdir, tzstr, otype, ext):
    """Print all messages from df into outdir

    Args:
        df: df with e-mail
        outdir: output directory

    Returns:
        Prints to outdir

    """

    mkdir_recursive(outdir)
    for thr in df['threadId'].unique():
        dfmsg = df.ix[df['threadId'] == thr]

        outdt     = dfmsg['date'][-1].strftime("%Y-%m-%d %H:%M " + tzstr)
        outsub    = dfmsg['subject'][-1][:32].replace('/', '|')
        outfolder = os.path.join(outdir, outdt + ' - ' + outsub)
        outpath   = filter(lambda x: x in string.printable, outfolder)

        mkdir_recursive(outpath)
        try:
            for i in dfmsg.index:
                print_df_msg(dfmsg.ix[i], outpath, tzstr, otype, ext)
        except:
            print_df_msg(dfmsg, outpath, tzstr, otype, ext)

def print_df_msg(dfmsg, dest, tzstr, otype, ext):
    """Print message out to file

    Args:
        p: Headers and body to print
        fout: Output folder

    Kwargs:
        pretty: Whether to make the body pretty using BS

    Returns:
        Prints p to file in fout
    """

    dstr = "%Y-%m-%d %H:%M " + tzstr
    try:
        h  = dfmsg['header'].values[0]
        fh = dfmsg['ft_header'].values[0]
        b  = dfmsg['body'].values[0]
        f  = pd.to_datetime(dfmsg['date'].values).strftime(dstr)[0]
        fn = dfmsg['fn'].values
        a  = dfmsg['att'].values
    except:
        h  = dfmsg['header']
        fh = dfmsg['ft_header']
        b  = dfmsg['body']
        fn = dfmsg['fn']
        a  = dfmsg['att']
        try:
            f = dfmsg['date'].strftime(dstr)
        except:
            f = dfmsg['date'][0].strftime(dstr)

    try:
        h  = unicode(h).encode('utf-8')
        fh = unicode(fh).encode('utf-8')
        b  = unicode(b).encode('utf-8')
    except:
        pass

    if otype != 'eml':
        with open(os.path.join(dest, f + ext), "wb") as fout:
            print(fh + os.linesep + os.linesep, file = fout)
            print(b, file = fout)

        if fn is not None:
            with open(os.path.join(dest, fn), "wb") as fout:
                if fn.endswith(' [ATTACHMENT T0O LARGE]'):
                    fout.write(a)
                else:
                    fout.write(base64.urlsafe_b64decode(a))

    if otype == 'eml' and fn is not None:
        msg_sep = '-q1w2e3r4t5'
        msg_c   = 'Content-type: multipart/mixed; boundary="%s"' % msg_sep
        hlist   = h.split(os.linesep)
        header  = os.linesep.join(hlist[:-1] + [msg_c])

        msg_h   = hlist[-1] + os.linesep + 'Content-Disposition: inline'
        att_h   = [u'Content-Type: application; name="%s"' % fn]
        att_h  += [u'Content-Transfer-Encoding: base64']
        att_h  += [u'Content-Disposition: attachment; filename="%s"' % fn]
        att_h   = os.linesep.join(att_h)

        with open(os.path.join(dest, f + ext), "wb") as fout:
            print(header + os.linesep, file = fout)

            print('--' + msg_sep, file = fout)
            print(msg_h + os.linesep, file = fout)
            print(b + os.linesep, file = fout)

            print('--' + msg_sep, file = fout)
            print(unicode(att_h).encode('utf-8') + os.linesep, file = fout)
            att_wrap = hard_wrap(a.replace('-', '+').replace('_', '/'), 76)
            print(att_wrap + os.linesep, file = fout)

            print('--' + msg_sep + '--', file = fout)
    elif otype == 'eml':
        with open(os.path.join(dest, f + ext), "wb") as fout:
            print(h + os.linesep, file = fout)
            print(b, file = fout)

def hard_wrap(text, width):
    nl  = int(len(text) / width)
    l   = 0
    hw  = [text[l * width:(l + 1) * width]]
    while l < nl:
        l  += 1
        hw += [text[l * width:(l + 1) * width]]

    return os.linesep.join(hw)

def apply_rules(srules, outdir, indir, infile, case):
    """Recursively applies rules in srules within outdir

    Args:
        srules: Dictionary with rules.
        outdir: Directory move the input file's folder.
        indir: Directory to move if infile matches rules.
        infile: Input file to apply the rules to.
        case: Whether regex is case-sensitive

    Returns:
        Each key in srules is a sub-folder within outdir where indir
        will be moved to if it infile matches the corresponding rule.
        Rules are applied in order as set by srules[key][priority].
        Keys with equal priority are applied in arbitrary order.
    """

    flat = [[k, v["priority"], v["rules"]] for k, v in srules.items()]
    for key, priority, rules in sorted(flat, key = itemgetter(1)):
        for i, line in enumerate(open(os.path.join(indir, infile))):
            for rule in rules:
                if case:
                    search = re.search(rule, line)
                else:
                    search = re.search(rule, line, re.IGNORECASE)

                if search:
                    mkdir_recursive(os.path.join(outdir, key))
                    move(indir, os.path.join(outdir, key))
                    return key

    return None

def mkdir_recursive(directory):
    try:
        os.makedirs(directory)
        return directory
    except OSError:
        if not os.path.isdir(directory):
            raise

# ---------------------------------------------------------------------
# Run the things


if __name__ == '__main__':
    main()
