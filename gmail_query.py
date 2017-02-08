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
have only tested it on my local Linux machine. My main concern is that
I don't append the .eml file extension to any messages, since my OS
automatically detects the file type. Windows in particular will probably
have a hard time with the downloaded format. To append a file extension
specify `-e eml`

I realize that pandas is a rather annoying dependence to have, but I
wanted to download threaded messages into the same subfolder and using
pandas seemed like the easiest way to group messages by thread. Feel
free to modify the code if you have a better idea.

Further, since I download messages by thread, I don't know a priori the
depth of the message thread or whether a "thread" is really a single
message. Hence all the awkward try/except pairs that try to find the
messages within a thread: An e-mail object can be a message or a thread
and this needs to handle both.

BeautifulSoup is a completely optional dependency. The only purpose it
serves is to print HTML messages in a pretty format. You might think
this makes no sense since the e-mail app opening the file won't care, but
I often look at my e-mails in plain text. It also makes searching your
much easier for me.
"""

from __future__ import division, print_function
from bs4 import BeautifulSoup as bs
from dateutil.parser import parse
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from dateutil import tz
from operator import itemgetter
from shutil import move
import pandas as pd
import oauth2client
import httplib2
import datetime
import base64
import string
import json
import os
import re

#################################
#  EDIT THIS IF YOU'RE NOT ME!  #
#################################

my_email   = "mauricio.caceres.bravo@gmail.com"
my_secret  = "client_secret.json"
my_appname = 'Gmail API Python Quickstart'

#################################
#  EDIT THIS IF YOU'RE NOT ME!  #
#################################

# ---------------------------------------------------------------------
# Parse arguments

try:
    import argparse
    parser = argparse.ArgumentParser(parents = [tools.argparser])

    parser.add_argument('-o', '--output',
                        dest     = 'out',
                        type     = str,
                        nargs    = 1,
                        metavar  = 'OUT',
                        default  = ['~/Documents/personal/99-email-dump'],
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

    parser.add_argument('-b', '--days-back',
                        dest     = 'days_back',
                        type     = int,
                        nargs    = 1,
                        metavar  = 'days_back',
                        default  = [0],
                        help     = "Days back to query e-mail.",
                        required = False)

    parser.add_argument('-e', '--ext',
                        dest     = 'ext',
                        type     = str,
                        nargs    = 1,
                        metavar  = 'ext',
                        default  = [''],
                        help     = "File extension.",
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

    parser.add_argument('--html',
                        dest     = 'html',
                        action   = 'store_true',
                        help     = "Parse resulting EML files into HTML files",
                        required = False)

    parser.add_argument('-s', '--sort',
                        dest     = 'sort',
                        action   = 'store_true',
                        help     = "Sort e-mail using sorting rules.",
                        required = False)

    parser.add_argument('--sort-rules',
                        dest     = 'sort_rules',
                        type     = str,
                        nargs    = 1,
                        metavar  = 'SORT_RULES',
                        default  = ['~/Documents/code/bin/gmail_rules.json'],
                        help     = "File with sorting rules.",
                        required = False)

    flags  = parser.parse_args()
    outdir = os.path.expanduser(flags.out[0])
    todays = flags.date[0]
    bdays  = flags.days_back[0]
    ext    = flags.ext[0]
    mail   = flags.mail
    html   = flags.html
    first  = flags.first
    sort   = flags.sort
    sort_rules = os.path.expanduser(flags.sort_rules[0])
except:
    flags  = None
    outdir = os.path.expanduser('~/Documents/personal/99-email-dump')
    todays = None
    ext    = ''
    bdays  = 0
    html   = False
    mail   = False
    first  = False
    sort   = False
    sort_rules = os.path.expanduser('~/Documents/code/bin/gmail_rules.json')

# Python 2/3 compat
# -----------------

try:
    unicode is unicode
except NameError:
    def unicode(x):
        return str(x, 'utf-8')

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

    def __init__(self, outdir = '~/Documents/personal/99-email-dump'):
        """Query gmail e-mail for the day

        Kwargs:
            outdir: Output directory
        """

        self.outmail  = my_email
        self.outdir   = outdir
        self.timezone = tz.tzlocal()
        self.tzstr    = datetime.datetime.now(self.timezone).tzname()

        # API info
        # --------

        client_secret_file = my_secret
        app_name = my_appname
        scopes   = ['https://www.googleapis.com/auth/gmail.readonly',
                    'https://www.googleapis.com/auth/gmail.insert']

        # Create gmail messages object
        # ----------------------------

        credentials   = get_credentials(app_name, client_secret_file, scopes)
        http          = credentials.authorize(httplib2.Http())
        service       = discovery.build('gmail', 'v1', http    = http)
        self.messages = service.users().messages()

    def query(self,
              todays = None,
              bdays  = 0,
              ext    = '',
              html   = False,
              mail   = False,
              first  = False,
              sort   = False,
              sort_rules = '~/Documents/code/bin/gmail_rules.json'):

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

        # Get date to query, recursively create output dir
        # ------------------------------------------------

        todays = todays if todays else str(datetime.date.today())
        outdir = os.path.join(self.outdir, todays)
        self.finaldir = outdir
        mkdir_recursive(outdir)

        # Query Gmail
        # -----------

        try:
            df  = self.query_todays(todays, bdays, first)
        except:
            df  = None
            res = "Gmail query FAILED"

        if df is not None:
            print_df_query(df, outdir, self.tzstr, html)
            if sort:
                if os.path.isfile(sort_rules):
                    try:
                        self.sort_query(sort_rules)
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

    def query_todays(self, todays, bdays, first):
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
        msg_ids     = set([ids['id'] for ids in todaym['messages']])
        all_msgs    = [self.get_msg(mid) for mid in msg_ids]
        thr_ids     = [msg['threadId'] for msg in all_msgs]
        all_atts    = [self.parse_att(msg) for msg in all_msgs]
        all_parsed  = [self.parse_msg(msg) for msg in all_msgs]

        # Return messages as pandas data frame
        cols  = ['threadId', 'body', 'header', 'date', 'subject', 'fn', 'att']
        dtzip = zip(thr_ids, all_parsed, all_atts)
        dfstr = '%Y-%m-%d %H:%M:%S ' + self.tzstr
        dt    = [[thr] + pmsg + att for thr, pmsg, att in dtzip]
        df    = pd.DataFrame(dt, index = msg_ids, columns = cols)

        # Parse string dates as datetime
        df['dtf'] = df['date'].apply(lambda d: d.strftime(dfstr))
        df.sort_values(by = ['threadId', 'dtf'],
                       inplace = True,
                       ascending = [True, not first])

        return df

    def sort_query(self, sort_rules):
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
                for f in files:
                    if apply_rules(srules, outdir, root, f) in srules.keys():
                        break

                if os.path.isdir(root):
                    move(root, unsorted)

    def parse_att(self, msg, depth = 10, msize = 20):
        """Get all attachments in message thread

        Args:
            msg: gmail msg

        Kwargs:
            depth: how deep to look for parts in payload
            msize: download attachments up do msize MiB

        Returns:
            All attachments found in msg, or None

        """

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
                att_fn  = part['filename']
                att_id, att_size = part['body'].values()
                att_mib = att_size / 1024 ** 2
                if att_mib < msize:
                    att  = self.get_att(msg['threadId'], att_id)
                    body = unicode(att['data']).encode('utf-8')
                    att_data = body
                else:
                    msg_size = 'NOTE: Att %.2f MiB but limit was %.1f MiB'
                    att_data = msg_size % (att_mib, msize)

        return [att_fn, att_data]

    def parse_msg(self, msg, prefer = 'text/html', depth = 10):
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
        header = ['From: ' + fr,
                  'To: ' + to,
                  'Cc: ' + cc if cc else cc,
                  'Subject: ' + sub,
                  'Date: ' + dates,
                  'Id: ' + msg['id'],
                  'Content-type: ' + pmime]

        return [plain, os.linesep.join(filter(None, header)), datel, sub]

    def get_msg(self, msg_id):
        return self.messages.get(userId = 'me',
                                 id     = msg_id,
                                 format = 'full').execute()

    def get_att(self, msg_id, att_id):
        return self.messages.attachments().get(userId = 'me',
                                               messageId = msg_id,
                                               id = att_id).execute()

def get_credentials(app_name, client_secret_file, scopes):
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

def print_df_query(df, outdir, tzstr, html):
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
                print_df_msg(dfmsg.ix[i], outpath, tzstr, html)
        except:
            print_df_msg(dfmsg, outpath, tzstr, html)

def print_df_msg(dfmsg, dest, tzstr, html, pretty = True):
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
        b  = dfmsg['body'].values[0]
        f  = pd.to_datetime(dfmsg['date'].values).strftime(dstr)[0]
        fn = dfmsg['fn'].values
        a  = dfmsg['att'].values
    except:
        h  = dfmsg['header']
        b  = dfmsg['body']
        fn = dfmsg['fn']
        a  = dfmsg['att']
        try:
            f = dfmsg['date'].strftime(dstr)
        except:
            f = dfmsg['date'][0].strftime(dstr)

    try:
        h = unicode(h).encode('utf-8')
        b = unicode(b).encode('utf-8')
    except:
        pass

    if pretty and h.find('text/html') > 0:
        try:
            b = bs(b, "lxml").prettify().encode('utf-8')
        except:
            pass

    if fn:
        msg_sep = '-q1w2e3r4t5'
        msg_c   = 'Content-type: multipart/mixed; boundary="%s"' % msg_sep
        hlist   = h.split(os.linesep)
        header  = os.linesep.join(hlist[:-1] + [msg_c])

        msg_h   = hlist[-1] + os.linesep + 'Content-Disposition: inline'
        att_h   = [u'Content-Type: application; name="%s"' % fn]
        att_h  += [u'Content-Transfer-Encoding: base64']
        att_h  += [u'Content-Disposition: attachment; filename="%s"' % fn]
        att_h   = os.linesep.join(att_h)

        # with open(os.path.join(dest, fn), "wb") as fout:
        #     fout.write(base64.urlsafe_b64decode(a))

        with open(os.path.join(dest, f), "wb") as fout:
            print(header + os.linesep, file = fout)

            print('--' + msg_sep, file = fout)
            print(msg_h + os.linesep, file = fout)
            print(b + os.linesep, file = fout)

            print('--' + msg_sep, file = fout)
            print(unicode(att_h).encode('utf-8') + os.linesep, file = fout)
            att_wrap = hard_wrap(a.replace('-', '+').replace('_', '/'), 76)
            print(att_wrap + os.linesep, file = fout)

            print('--' + msg_sep + '--', file = fout)
    else:
        with open(os.path.join(dest, f), "wb") as fout:
            print(h + os.linesep, file = fout)
            print(b, file = fout)

    if html:
        hparse = "/home/mauricio/Documents/code/bin/gmail_query_to_html.rb"
        os.system(hparse + ' "' + os.path.join(dest, f) + '"')

def hard_wrap(text, width):
    nl  = int(len(text) / width)
    l   = 0
    hw  = [text[l * width:(l + 1) * width]]
    while l < nl:
        l  += 1
        hw += [text[l * width:(l + 1) * width]]

    return os.linesep.join(hw)

def apply_rules(srules, outdir, indir, infile):
    """Recursively applies rules in srules within outdir

    Args:
        srules: Dictionary with rules.
        outdir: Directory move the input file's folder.
        indir: Directory to move if infile matches rules.
        infile: Input file to apply the rules to.

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
                if re.search(rule, line, re.IGNORECASE):
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
    query  = gmail_query(outdir)
    query.query(todays = todays,
                bdays  = bdays,
                ext    = ext,
                html   = html,
                mail   = mail,
                first  = first,
                sort   = sort,
                sort_rules = sort_rules)
