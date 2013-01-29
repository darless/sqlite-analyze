#!/usr/bin/python

__author__ = "Nodar Nutsubidze"

import os, sys, time, socket, threading, string, shutil
import sqlite3
import argparse

class sql_db():
  """
  Main DB info
  """
  def __init__(self, path):
    self.path = path
    self.tbl_arr = []

    # Size in bytes of the DB
    self.size = 0

  def add_tbl(self, tbl_obj):
    self.tbl_arr.append(tbl_obj)

  def sort(self):
    """
    Sorts the self.tbl_arr by the table size
    """
    new_arr = self.tbl_arr
    new_arr.sort(tbl_size_compare)
    if len(new_arr) != len(self.tbl_arr):
      log("Sorting failed old: [{}] -> new: [{}] - Reverting".format(len(self.tbl_arr), len(new_arr)))
    else:
      self.tbl_arr = new_arr

  def get_tbl_info(self):
    """
    By looking at the db path it parses
    out the tables and the number of entries
    in each table
    """
    if not os.path.exists(self.path):
      return

    self.size = os.stat(self.path).st_size

    conn = sqlite3.connect(self.path)
    c = conn.cursor()

    tbl_arr = get_tbl_arr(self.path)
    for tbl in tbl_arr:
      obj = sql_db_tbl(tbl)
      c = conn.cursor()
      c.execute("select count(*) from {}".format(tbl))
      obj.num_entries = c.fetchone()[0]
      self.add_tbl(obj)
    conn.close()

  def get_num_entries(self):
    """
    Returns total number of entries in the entire table.
    """
    total = 0
    for tbl in self.tbl_arr:
      total += tbl.num_entries;
    return total;

class sql_db_tbl():
  """
  Table info
  """
  def __init__(self, name):
    self.name = name
    self.num_entries = 0
    self.tbl_size = 0
    self.entry_size = 0

def tbl_size_compare(a, b):
  """
  Compares to tbl objectes based on the size
  """
  return cmp(a.tbl_size, b.tbl_size)

def print_usg():
  log("Usage: {} \n".format(sys.argv[0]))
  return

def log(str):
  sys.stdout.write(str+"\n")

def clean_utf(data):
  if data is None:
    return data
  return data.encode('utf8')

def get_tbl_arr(db):
  """
  Returns an array of tables given a DB.
  """
  if db is None:
    log("perform_basic: No DB passed in")
    return
  result_arr = []
  conn = sqlite3.connect(db)
  c = conn.cursor()
  arr = c.execute("select name from sqlite_master where type='table'")
  for row in arr:
    tbl = row[0].encode('utf8')
    result_arr.append(tbl)
  conn.close()
  return result_arr

def perform_basic(db):
  """
  All this does is given a DB returns the number of
  entries in each table
  """
  if db is None:
    log("perform_basic: No DB passed in")
    return
  db_stats = sql_db(db)
  db_stats.get_tbl_info()
  print "{:<16}: {}".format("Table", "# of entries")
  print "--------------------------------------"
  for tbl in db_stats.tbl_arr:
    print "{:<16}: {}".format(tbl.name, tbl.num_entries)
  print "--------------------------------------"
  print "Total # of entries: {}".format(db_stats.get_num_entries())

def perform_detail(db):
  """
  The purpose here is to figure out how much space each entry takes up.
  """
  tmp_db = ".tmp_db.db"
  dump_file = ".dump_db"
  schema_db = ".tmp_schema.db"

  db_stats = sql_db(db)
  db_stats.get_tbl_info()

  # Go through tables and for each one just
  # dump from the main one to a tmp file
  # Then import it into the new db
  for tbl in db_stats.tbl_arr:
    print "Parsing table {}".format(tbl.name)
    if os.path.exists(tmp_db):
      os.remove(tmp_db)
    os.system("sqlite3 {} '.dump {}' > {}".format(db, tbl.name, dump_file))
    os.system("sqlite3 {} < {}".format(tmp_db, dump_file))
    tbl.tbl_size = os.stat(tmp_db).st_size
    if tbl.num_entries <= 0:
      # Since there are no entries then the tbl_size is just the
      # schema so reset it to 0
      tbl.tbl_size = 0
    else:
      tbl.entry_size = tbl.tbl_size / tbl.num_entries

  # Sort the results
  db_stats.sort()

  print "\n\n"
  print "DB Size: {}".format(readable(db_stats.size))
  print "\n"
  print "{:<16}: {:<16} {:<16} {:<16} {:<16}".format("Table", "# entries", "Size", "Entry Size", "Percent")
  dash = '-'.rjust(16, '-')
  print "{:<16}--{:<16}-{:<16}-{:<16}-{:<16}".format(dash, dash, dash, dash, dash)

  # Print the results
  for tbl in db_stats.tbl_arr:
    num = tbl.num_entries
    tbl_size = readable(tbl.tbl_size)
    entry_size = readable(tbl.entry_size)
    percent = (float(tbl.tbl_size) / db_stats.size) * 100
    percent = "{} %".format(round(percent, 2))

    print "{:<16}: {:<16} {:<16} {:<16} {:<16}".format(tbl.name, num, tbl_size, entry_size, percent)

def readable(size, num_round=2):
  """
  Takes a size in bytes and converts that to
  a readable string such as 5.3 MB
  """
  cur_type = 0
  type_arr = ["B", "KB", "MB", "GB", "TB", "PB"]
  size = float(size)
  while size >= 1000:
    if cur_type >= len(type_arr):
      break;
    size = size / 1000
    cur_type += 1
  size = round(size, num_round)
  return "{} {}".format(size, type_arr[cur_type])

def main():
  parser = argparse.ArgumentParser(description = 'Analyze a SQLite3 DB')
  parser.add_argument('action', metavar='action', help="basic or detail")
  parser.add_argument('db', metavar='db', help='database')
  if len(sys.argv) <= 1:
    parser.print_help() 
    return
  args = parser.parse_args()
  if args.action == "basic":
    perform_basic(args.db)
  elif args.action == "detail":
    perform_detail(args.db)
  else:
    log("Unknown action {}".format(args.action))
    
  
  sys.exit(0); 

main()
