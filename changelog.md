Change Log
==========

## gmail-download-0.1.0 (2017-02-09)

### Features

* Download Gmail e-mail, optionally sorting into folders. See README.md
* Downloading attachments now optional
* Can specify maximum attachment size
* Configuration now in `~/.gmail_query.conf`
* Program creates configuration if none found
* Can update configuration via `setup`
* Can download e-mails to any format supported by `pandoc`
* No sorting if no file is provided

### Known bugs

* Cannot handle multiple attachments.
