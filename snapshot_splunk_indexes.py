#!/usr/bin/python

# get a snapshot of the oldest, latest, disk storage size footprint per Splunk index
# send csv report

import subprocess
import os
import sys
import csv
import fnmatch
import ConfigParser
import traceback
import time
import smtplib
import pdb
import json
import socket
import re
import math
import pprint
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from mimetypes import guess_type
from cStringIO import StringIO
from email.encoders import encode_base64

host = str(socket.gethostname())
cname = host.split('.')[0]
my_csv = 'index.summary.' +cname +time.strftime(".%m%d%Y") +'.csv'
my_summary = 'index.summary.size.' +cname +time.strftime(".%m%d%Y") +'.csv'

cfg_pathd = '/opt/splunk/etc/apps/SFDC_all_indexer_base/default/indexes.conf'
index_path = '/data'

# build index list
indexes = {}

header = ['size', 'bucket_ts_count', 'config_unit', 'true_path', 'config_size', 'retention', 'expected_latest', 'dir', 'earliest', 'size_unit', 'latest']
errata = list()

pp = pprint.PrettyPrinter(indent=2)

def check_summary_item(idx, k):
  flag = True

  for i in header:
    if i not in idx[k].keys():
      print k, ' => ', idx[k].keys()
      errata.append('ERROR: key %s DNE for index section %s' % (i, k))
      flag = False
  return flag

def build_summary_file(config_total):
  with open (my_summary, 'wb') as f:
    csvwriter = csv.writer(f)
    csvwriter.writerow(['host', 'path', 'used', 'available', 'total', 'unit', 'use%', 'config_total_tb'])
    #pdb.set_trace()
    du = os.popen("df -h " +index_path +" | tail -n +2 | awk '{print $NF, $3, $4, $2, $5}'").read()
    u = re.search('(G|T|M)', du)
    du = re.sub(r'(G|T|\\n)', r'', du).split()
    #pdb.set_trace()
    ln = du[:-1] +[u.group(1)+'B'] +du[-1:] +["{0:.3f}".format(config_total)]
    csvwriter.writerow([cname] +ln)
  return csvwriter

def build_csv_file(summary):
  global header
  #print 'printing summary keys: \n', summary[summary.keys()[0]].keys()
  header = summary[summary.keys()[0]].keys()
  with open (my_csv, 'wb') as f:
    #pdb.set_trace()
    wf = csv.writer(f)
    wf.writerow(['host','index'] +header)
    for k in sorted(summary.keys()):
      if not check_summary_item(summary, k):
        continue
      ln = [cname, k] + summary[k].values()
      wf.writerow(ln)
  return wf

def attach_file(filename):
  mimetype, encoding = guess_type(filename)
  mimetype = mimetype.split('/', 1)
  fp = open(filename, 'rb')
  attachment = MIMEBase(mimetype[0], mimetype[1])
  attachment.set_payload(fp.read())
  fp.close()
  encode_base64(attachment)
  attachment.add_header('Content-Disposition', 'attachment',
    filename=os.path.basename(filename))
  return attachment

def send_report(summary, config_total, errs=None):
  SERVER = 'localhost'
  RECEIVER = 'hcanivel@salesforce.com'
  FROM = 'splunk.index.report@' +host
  TO = [RECEIVER] # must be a list

  SUBJECT = 'Splunk indexing report: ' + cname

  TEXT = 'Report listing per Splunk index metrics for server '
  TEXT += str(cname)
  TEXT += ':\n\r* timestamp of earliest and latest bucket (event)'
  TEXT += '\n\t* size of index in local instance (will be diff per indexer)'

  #json.dumps(summary, indent=4)
  #json.dumps(errs, indent=4)
  if errs:
    TEXT += '\n\nErrata:\n'
    for e in errs:
      TEXT += '\n\t* ' + e
    
  TEXT += str('\n\nSincerely,\nSplunk Index Report')
  
  # Prepare actual message
  msg = MIMEMultipart()
  msg['Subject'] = SUBJECT
  msg['From'] = FROM
  msg['To'] = ','.join(TO)
  msg.attach(MIMEText(TEXT, 'plain'))
  
  # produce csv summary
  #pdb.set_trace()
  if summary:
    # Attach csvs
    volsum = build_csv_file(summary)
    at1 = attach_file(my_csv)
    msg.attach(at1)
  
    summ = build_summary_file(config_total)
    at2 = attach_file(my_summary)
    msg.attach(at2)
  
  #return
  # Send the mail
  s = smtplib.SMTP(SERVER)
  s.sendmail(FROM, TO, msg.as_string())
  s.quit()

try:
  configd = ConfigParser.ConfigParser()
  cfgd = configd.read(cfg_pathd)

  config_total = 0.0

  #pdb.set_trace()
  for section in configd.sections():
    # skip default setting
    if section == 'default':
      continue
    indexes[section] = {}
    # expected: '$SPLUNK_DB/<section>/db'
    try:
      indexes[section]['dir'] = configd.get(section, 'homepath')
    except:
      # splunk internal dbs 
      if section == '_audit':
        indexes[section]['dir'] = index_path +'/' +'audit'
      elif section == '_internal':
        indexes[section]['dir'] = index_path +'/' +'_internaldb'
      else:
        indexes[section]['dir'] = index_path +'/' +section

    # provide true path (substitute $SPLUNK_DB)
    indexes[section]['true_path'] = indexes[section]['dir'].replace('$SPLUNK_DB', index_path)

    # validate section in path
    idx_check = indexes[section]['dir'].split('/')[1]
    if section != idx_check:
      errata.append('FYI: index section %s does not match folder name %s' % (section, idx_check))

    ed = list()
    # set default values
    indexes[section]['actual_size'] = 0
    indexes[section]['actual_unit'] = 'MB'
    folder_size = indexes[section]['actual_size']
    indexes[section]['earliest'] = None
    indexes[section]['latest'] = None
    indexes[section]['bucket_ts_count'] = len(ed)
    indexes[section]['config_size'] = 40000/1000
    indexes[section]['config_unit'] = 'GB'
    indexes[section]['retention'] = str(float(configd.get('default', 'frozentimeperiodinsecs'))/86400) + ' days'
    indexes[section]['size %'] = '0'

    # pull config size
    # maxTotalDataSizeMB = 40000
    config_size_gb = 40
    if configd.has_option(section, 'maxTotalDataSizeMB'):
      config_size = int(configd.get(section, 'maxTotalDataSizeMB'))/1000
      config_size_gb = int(config_size)
      #pdb.set_trace()
      if config_size >= 1000:
        config_size = config_size/1000
        indexes[section]['config_unit'] = 'TB'
      indexes[section]['config_size'] = config_size

    # validate if folder exists
    if not os.path.isdir(indexes[section]['true_path']):
      #pdb.set_trace()
      errata.append("ERROR: folder '%s' not found" % indexes[section]['true_path'])
      continue

    # event timestamps in splunk bucket effectively follow the schema: <latest ts>-<earliest ts>-<idnum>.tsidx
    # extract all timestamps into
    for root, dirnames, filenames in os.walk(indexes[section]['true_path']):
      for filename in filenames:
        fp = os.path.join(root, filename)
        fs = os.path.getsize(fp)
        folder_size += fs
        # fp, fs, folder_size
        if filename in fnmatch.filter(filenames, '*tsidx'):
          latest_event = filename.split('-')[0]
          earliest_event = filename.split('-')[1]
          ed.append(latest_event)
          ed.append(earliest_event)

    if not ed:
      errata.append('FYI: Nothing found for %s' % section)
      #pdb.set_trace()
      continue
    ed = sorted(set(ed))

    # capture retention 
    sec = configd.get(section, 'frozentimeperiodinsecs')
    indexes[section]['retention'] = str(float(sec)/86400) + ' days'

    # set latest/earliest for this index
    indexes[section]['earliest'] = time.strftime("%D %T %Z", time.gmtime(int(ed[0])) )
    indexes[section]['latest'] = time.strftime("%D %T %Z", time.gmtime(int(ed[-1])) )
    indexes[section]['bucket_ts_count'] = len(ed)

    # set folder size (MB)
    readable_fs = folder_size/1024.0/1024.0
    readable_fs_gb = readable_fs/1024.0
    indexes[section]['actual_unit'] = 'MB'
    # check if GB or MB size
    if folder_size > (1024.0**3):
      readable_fs = readable_fs/1024.0
      indexes[section]['actual_unit'] = 'GB'
    indexes[section]['actual_size'] = "{0:.3f}".format(readable_fs)

    # calculate used percentage
    perc = readable_fs/indexes[section]['config_size']

    # if configured unit diff from real, adjust
    used_sec = int(ed[-1]) - int(ed[0])
    used_days = used_sec/86400.0
    if indexes[section]['actual_unit'] != indexes[section]['config_unit']:
      perc = perc/1024.0
    indexes[section]['size %'] = "{0:.3f}".format(perc*100)
    indexes[section]['days_used'] = "{0:.3f}".format(used_days) + '/' +str(float(sec)/86400)
    indexes[section]['days_used %'] = "{0:.3f}".format(float(used_sec)/float(sec)*100)

    index_rate_day = readable_fs_gb/(used_sec/86400.0)
    days_expected = config_size_gb/index_rate_day

    config_total += config_size_gb/1024.0
    
    # capture expected termination
    indexes[section]['expected_rate_daily'] = "{0:.6f}".format(index_rate_day)
    indexes[section]['expected_day_count'] = "{0:.3f}".format(days_expected)
    indexes[section]['expected_latest_by_date'] = time.strftime("%D %T %Z", time.gmtime(int(ed[0]) + int(sec)) )
    indexes[section]['expected_latest_by_volume'] = time.strftime("%D %T %Z", time.gmtime(int(ed[0]) + days_expected*86400) )

    #if 'sec' in section:
    #  print "index: %s" % section
    #  print "sec: %i\tused_sec: %f\tused_days: %f\treadable_fs_gb: %f\tindex_rate_day: %.3f\tconfig_size_gb: %i\tdays_expected: %i" \
    #    % (int(sec), used_sec, used_days, readable_fs_gb, index_rate_day, config_size_gb, days_expected)
    #  pp.pprint(indexes[section])

    print "section: %s\tadding: %f\ttotal: %f" % (section, config_size_gb, config_total)
  #pdb.set_trace()
  #print section, indexes[section]['earliest'],indexes[section]['latest'],indexes[section]['size']
  #print len(errata), errata
  send_report(indexes, config_total, errata)
except:
  exc_type, exc_value, exc_traceback = sys.exc_info()
  traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
  traceback.print_exception(exc_type, exc_value, exc_traceback,
                                limit=2, file=sys.stdout)
  #print 'Something failed: %s' % (str(e))
