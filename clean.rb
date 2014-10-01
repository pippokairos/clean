#!/usr/bin/env ruby

require "yaml"
require "pg"
require "fileutils"
require "json"
require "zlib"
require "archive/tar/minitar"
include Archive::Tar

def is_int?(str) # method to define if a string represents an integer
  !!(str =~ /\A[-+]?[0-9]+\z/)
end

def is_valid_json?(str) # method to define if a string is a valid json
  JSON.parse(str)
    return true
  rescue JSON::ParserError
    return false
end

def add_quotes(str)
  "\"#{str}\""
end

def main
  config = YAML.load_file("config.yml")

  database = config["database"]
  table = config["table"]
  includes = config["include"]

  conn = PG.connect(dbname: "#{database}")

  includes.each do |field|
    rows = conn.exec("SELECT #{field} FROM #{table}")

    FileUtils.mkdir_p "files" # mkdir if not existing
    json_file_name = "files/#{database}_#{table}_#{field}.json"

    File.open(json_file_name, "a") { |out| 
      out.truncate(0) # delete file content 
      out.print("{\"#{field}\":\n  [\n")

      rows.each_with_index do |row, index|
        value = row.values[0]
        if index == rows.count - 1 # last row
          (is_int?(value) || is_valid_json?(value)) ? out.print("    #{value}\n") : out.print("    #{add_quotes(value)}\n")
        else
          (is_int?(value) || is_valid_json?(value)) ? out.print("    #{value},\n") : out.print("    #{add_quotes(value)},\n")
        end
      end

      out.print("  ]\n}")
    }

    puts "#{rows.count} rows written to #{json_file_name}"
  end

  table_vals = conn.exec("SELECT * FROM #{table}")

  stock_file_name = "files/#{config["stock_file_name"]}"

  File.open(stock_file_name, "a") { |out|
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

  conn.exec("BEGIN") # for testing
  conn.exec("DELETE FROM #{table}")
  puts "#{table_vals.cmd_tuples()} rows deleted from the table #{table}"
  conn.exec("ROLLBACK") # for testing
end

main()
