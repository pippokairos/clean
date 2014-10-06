#!/usr/bin/env ruby

require "yaml"
require "pg"
require "fileutils"
require "json"
require "zlib"
require "archive/tar/minitar"
include Archive::Tar

def int?(str) # method to define if a string represents an integer
  !!(str =~ /\A[-+]?[0-9]+\z/)
end

def valid_json?(str) # method to define if a string is a valid json
  JSON.parse(str)
    return true
  rescue JSON::ParserError
    return false
end

def validate(str) # method to add quotes to non-int and non-json fields
  str = (int?(str) || valid_json?(str)) ? str : "\"#{str}\""
end

def write_multiple_json_files(connection, database, table, includes)
  FileUtils.mkdir_p "files" # mkdir if not existing
  json_file_name = "files/#{database}_#{table}_includes.json"

  File.open(json_file_name, "a") { |out|
    out.print("{\n")
    includes.each_with_index do |field, field_index|
      rows = connection.exec("SELECT #{field} FROM #{table}")
      out.print("  \"#{field}\":[\n")

      rows.each_with_index do |row, row_index|
        value = row.values[0]
        end_of_line = (row_index == rows.count - 1) ? '' : ',' # no comma on last row
        out.print("    #{validate(value)}#{end_of_line}\n")
      end

      field_index == includes.count - 1 ? out.print("  ]\n") : out.print("  ],\n") 
      puts "#{rows.count} rows written to #{json_file_name} for the field #{field}"
    end
    out.print("}")
  }
end

def write_single_json_file(connection, database, table, includes)
    includes.each do |field|
      rows = connection.exec("SELECT #{field} FROM #{table}")
  
      FileUtils.mkdir_p "files" # mkdir if not existing
      json_file_name = "files/#{database}_#{table}_#{field}.json"
  
      File.open(json_file_name, "w") { |out| 
        out.print("{\"#{field}\":\n  [\n")
  
        rows.each_with_index do |row, row_index|
          value = row.values[0]
          end_of_line = (row_index == rows.count - 1) ? '' : ',' # no comma on last row
          out.print("    #{validate(value)}#{end_of_line}\n")
        end
        out.print("  ]\n}")
      }
      puts "#{rows.count} rows written to #{json_file_name}"
  end
end

def stock_rows(connection, database, table, stock_file_name, delete_check)
  table_vals = connection.exec("SELECT * FROM #{table}")

  File.open(stock_file_name, "w") { |out|
    table_vals.each do |row|
      out.print("{")
      row.values.each_with_index do |value, index|
        index == row.values.count - 1 ? out.print("#{value}},\n") : out.print("#{value}, ")
      end
    end
  }

  tgz = Zlib::GzipWriter.new(File.open("stock.tgz", "wb")) # create the archive
  Minitar.pack(stock_file_name, tgz)                       # copy the stock file in it
  File.delete(stock_file_name)                             # and delete the file

  puts "#{table_vals.cmd_tuples()} rows stocked in tar archive"
  
  if delete_check
    rows = conn.exec("DELETE FROM #{table}")
    puts "#{rows.cmd_tuples()} rows deleted from the table #{table}"
  end
  
end

def main
  config = YAML.load_file("config.yml")

  database = config["database"]
  table = config["table"]
  includes = config["include"]

  conn = PG.connect(dbname: "#{database}")
  
  write_single_json_file(conn, database, table, includes)
  write_multiple_json_files(conn, database, table, includes)
  
  stock_file_name = "files/#{config["stock_file_name"]}"
  stock_rows(conn, database, table, stock_file_name, false)
end

main()
